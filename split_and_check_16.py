#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import msgpack
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed  # ✅ 这里一定要有
import time
import pickle

# ===============================
# 配置区（Config）
# ===============================
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"

PARTS = 16
DNS_TIMEOUT = 2
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.bin")
NOT_WRITTEN_FILE = os.path.join(DIST_DIR, "not_written_counter.bin")
RETRY_FILE = os.path.join(DIST_DIR, "retry_rules.txt")
DELETE_THRESHOLD = 4
DNS_BATCH_SIZE = 540
WRITE_COUNTER_MAX = 6
DNS_THREADS = 80


os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 文件确保函数（写入空 msgpack dict）
# ===============================
def ensure_bin_file(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        try:
            with open(path, "wb") as f:
                f.write(msgpack.packb({}, use_bin_type=True))
        except Exception as e:
            print(f"⚠ 初始化 {path} 失败: {e}")

ensure_bin_file(DELETE_COUNTER_FILE)
ensure_bin_file(NOT_WRITTEN_FILE)
if not os.path.exists(RETRY_FILE):
    open(RETRY_FILE, "w", encoding="utf-8").close()

# ===============================
# 二进制读取（msgpack）
# ===============================
def load_bin(path, print_stats=False):
    if os.path.exists(path):
        try:
            with open(path, "rb") as f:
                raw = f.read()
                if not raw:
                    return {}
                data = msgpack.unpackb(raw, raw=False)
            return data
        except Exception as e:
            print(f"⚠ 读取 {path} 错误: {e}")
            return {}
    return {}

# ===============================
# 二进制写入（msgpack）
# ===============================
def save_bin(path, data):
    try:
        with open(path, "wb") as f:
            f.write(msgpack.packb(data, use_bin_type=True))
    except Exception as e:
        print(f"⚠ 保存 {path} 错误: {e}")

# ===============================
# 打印 not_written_counter 统计（单独函数）
# ===============================
def print_not_written_stats():
    data = load_bin(NOT_WRITTEN_FILE)
    flat_counts = {}
    total_rules = 0
    for part_rules in data.values():
        if not isinstance(part_rules, dict):
            continue
        for cnt in part_rules.values():
            total_rules += 1
            c = min(int(cnt), WRITE_COUNTER_MAX)
            flat_counts[c] = flat_counts.get(c, 0) + 1
    print("📊 not_written_counter 概览:")
    print(f"    ℹ️ 总规则: {total_rules}")
    for k in sorted(flat_counts.keys()):
        print(f"    ⚠ write_counter={k}: {flat_counts[k]} 条")
    return flat_counts

# ===============================
# 单条规则 DNS 验证
# ===============================
def check_domain(rule):
    resolver = dns.resolver.Resolver()
    resolver.timeout = DNS_TIMEOUT
    resolver.lifetime = DNS_TIMEOUT
    domain = rule.lstrip("|").split("^")[0].replace("*", "")
    if not domain:
        return None
    try:
        resolver.resolve(domain)
        return rule
    except Exception:
        return None


# ===============================
# 下载并合并规则源
# ===============================
def download_all_sources():
    """
    下载所有规则源，合并规则，过滤并更新删除计数 delete_counter

    最终逻辑：
      - 新规则 delete_counter = 4
      - delete_counter <7 的规则进入 DNS 验证
      - delete_counter >=7 的规则:
            下载阶段：delete_counter += 1，不进入验证
            delete_counter >=24 且 rule ∈ all_rules → 重置为 6（重新进入验证）
      - delete_counter >=28 → 删除记录
      - retry_rules.txt 不在此处理
    """
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False

    print("📥 下载规则源...")
    all_rules = []
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]

    for url in urls:
        print(f"🌐 获取 {url}")
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            new_rules = [line.strip() for line in r.text.splitlines() if line.strip()]
            all_rules.extend(new_rules)
            print(f"🔄 下载 {url} 成功，获取 {len(new_rules)} 条规则")
        except requests.RequestException as e:
            print(f"⚠ 下载失败 {url}: {e}")

    print(f"✅ 合并 {len(all_rules)} 条规则")
    all_rules_set = set(all_rules)

    # 载入 delete_counter
    delete_counter = load_bin(DELETE_COUNTER_FILE) if os.path.exists(DELETE_COUNTER_FILE) else {}

    updated_delete_counter = {}
    filtered_rules = []       # <7 的规则，进入验证
    reset_rules = []          # 被重置为6 的规则
    removed_rules = []        # 被丢弃的规则
    skipped_rules = []        # >=7 的规则（下载阶段跳过验证）

    # ================================
    # 处理已有 delete_counter
    # ================================
    for rule, cnt in delete_counter.items():
        cnt = int(cnt)

        if cnt >= 7:
            # >=7 下载阶段 +1，不进入验证
            cnt += 1

            # delete_counter >=24 且仍在 all_rules → 重置为 6
            if cnt >= 24 and rule in all_rules_set:
                cnt = 6
                reset_rules.append(rule)

            # delete_counter >=28 且不在规则源 → 删除
            if cnt >= 28 and rule not in all_rules_set:
                removed_rules.append(rule)
                continue  # 不记录

            updated_delete_counter[rule] = cnt
            skipped_rules.append(rule)

        else:
            # cnt <7 → 能验证
            # 若 >=24（理论上不会出现），按逻辑处理
            if cnt >= 24 and rule in all_rules_set:
                cnt = 6
                reset_rules.append(rule)

            updated_delete_counter[rule] = cnt
            if cnt < 7:
                filtered_rules.append(rule)

    # ================================
    # 处理新规则
    # ================================
    for rule in all_rules:
        if rule not in updated_delete_counter:
            updated_delete_counter[rule] = 4
            filtered_rules.append(rule)

    # 保存 delete_counter
    save_bin(DELETE_COUNTER_FILE, updated_delete_counter)

    # ================================
    # 输出信息
    # ================================
    if reset_rules:
        print(f"🔁 共 {len(reset_rules)} 条规则 delete_counter≥24 且重新出现，已重置为 6")

    if removed_rules:
        print(f"🗑️ 共 {len(removed_rules)} 条规则 delete_counter≥28，已删除记录")

    if skipped_rules:
        print(f"⚠ 共 {len(skipped_rules)} 条规则 delete_counter≥7，在下载阶段被跳过验证")

    print(
        f"📚 合并总规则 {len(all_rules)} 条，"
        f"⏩跳过 {len(skipped_rules)} 条（delete_counter≥7），"
        f"🧮 需要验证 {len(filtered_rules)} 条（delete_counter<7），"
        f"🪓 即将切分为 {PARTS} 片"
    )

    # 切分进入验证的规则
    split_parts(filtered_rules, updated_delete_counter)
    return True

# ===============================
# 分片 + 负载均衡优化
# ===============================
def split_parts(all_rules, delete_counter):
    """
    根据现有 validated_part_X.txt 分片重新分配规则：
      - 剔除 delete_counter >=7 的规则
      - 保留原分片 delete_counter 值小的规则
      - 新规则均衡分配
      - delete_counter 大的规则尽量移动到负载轻分片
      - 最终每片数量差距 <=1
    """
    # 读取当前每个分片的已有规则
    part_existing = {}
    for i in range(1, PARTS + 1):
        f = os.path.join(DIST_DIR, f"validated_part_{i}.txt")
        if os.path.exists(f):
            with open(f, "r", encoding="utf-8") as ff:
                part_existing[i] = set(l.strip() for l in ff if l.strip())
        else:
            part_existing[i] = set()

    # 过滤 delete_counter >=7 的规则
    filtered_rules = [r for r in all_rules if int(delete_counter.get(r, 4)) < 7]

    # 先把规则分为“已有分片规则”和“新规则”
    rule_to_part = {}
    for r in filtered_rules:
        assigned = False
        for i in range(1, PARTS + 1):
            if r in part_existing[i]:
                rule_to_part[r] = i
                assigned = True
                break
        if not assigned:
            rule_to_part[r] = None  # 新规则

    # 初始化分片桶
    part_buckets = {i: [] for i in range(1, PARTS + 1)}

    # 先将原有分片规则按 delete_counter 小的优先放回原分片
    for r, p in rule_to_part.items():
        if p:
            part_buckets[p].append((r, int(delete_counter.get(r, 4))))

    # 新规则按 delete_counter 小优先，分配到规则最少的分片
    new_rules = [(r, int(delete_counter.get(r, 4))) for r, p in rule_to_part.items() if p is None]
    new_rules.sort(key=lambda x: x[1])
    for r, cnt in new_rules:
        # 找最少规则的分片
        target = min(part_buckets.items(), key=lambda x: len(x[1]))[0]
        part_buckets[target].append((r, cnt))

    # 优化 delete_counter 大的规则，尽量移动到负载轻分片
    max_iterations = 5
    for _ in range(max_iterations):
        counts = [len(part_buckets[i]) for i in range(1, PARTS + 1)]
        avg = sum(counts) / PARTS
        moved = False
        for i in range(1, PARTS + 1):
            bucket = part_buckets[i]
            # delete_counter 大到小排序
            bucket_sorted = sorted(bucket, key=lambda x: x[1], reverse=True)
            for r, cnt in bucket_sorted:
                # 找最小负载分片
                target = min(((k, len(part_buckets[k])) for k in range(1, PARTS + 1) if k != i), key=lambda x: x[1])[0]
                if len(bucket) - 1 >= avg and len(part_buckets[target]) + 1 <= avg:
                    bucket.remove((r, cnt))
                    part_buckets[target].append((r, cnt))
                    moved = True
                    break
            if moved:
                break
        if not moved:
            break

    # 写入 TMP_DIR/part_XX.txt
    os.makedirs(TMP_DIR, exist_ok=True)
    for i in range(1, PARTS + 1):
        filename = os.path.join(TMP_DIR, f"part_{i:02d}.txt")
        rules_only = [r for r, cnt in sorted(part_buckets[i], key=lambda x: x[0])]
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(rules_only))
        print(f"📄 分片 {i}: {len(rules_only)} 条规则 → {filename}")

# ===============================
# DNS 验证
# ===============================
def dns_validate(rules, part):
    """
    对给定规则集进行 DNS 验证，并返回有效的规则列表。
    注意：本函数不会自动读取 retry_rules.txt —— retry 规则只由 process_part() 在每次分片执行时加入。
    """
    combined_rules = rules
    tmp_file = os.path.join(TMP_DIR, f"vpart_{part}.tmp")
    with open(tmp_file, "w", encoding="utf-8") as f:
        f.write("\n".join(combined_rules))

    valid_rules = []
    total_rules = len(combined_rules)
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=DNS_THREADS) as executor:
        futures = {executor.submit(check_domain, r): r for r in combined_rules}
        completed = 0
        for future in as_completed(futures):
            try:
                res = future.result()
            except Exception:
                res = None
            if res:
                valid_rules.append(res)
            completed += 1
            if completed % DNS_BATCH_SIZE == 0 or completed == total_rules:
                elapsed = time.time() - start_time
                speed = completed / elapsed if elapsed > 0 else 0
                eta = (total_rules - completed) / speed if speed > 0 else 0
                print(f"✅ 已验证 {completed}/{total_rules} 条 | 有效 {len(valid_rules)} 条 | 速度 {speed:.1f}/秒 | 预计完成 {eta:.1f}s")

    return valid_rules

# ===============================
# 更新 not_written_counter
# ===============================
def update_not_written_counter(part_num, valid_rules, all_rules):
    part_key = f"validated_part_{part_num}"
    counter = load_bin(NOT_WRITTEN_FILE)

    for i in range(1, PARTS + 1):
        counter.setdefault(f"validated_part_{i}", {})

    validated_file = os.path.join(DIST_DIR, f"{part_key}.txt")
    existing_rules = set(open(validated_file, "r", encoding="utf-8").read().splitlines()) if os.path.exists(validated_file) else set()
    all_rules = set(all_rules)
    part_counter = counter.get(part_key, {})

    # 新验证成功规则 -> 重置计数
    for r in valid_rules:
        part_counter[r] = WRITE_COUNTER_MAX

    valid_rules_set = set(valid_rules)

    # 旧规则但本次没出现 -> 递减
    for r in existing_rules - valid_rules_set:
        part_counter[r] = max(part_counter.get(r, WRITE_COUNTER_MAX) - 1, 0)

    # write_counter == 1 且不在 all_rules -> 删除
    to_remove = []
    for r in list(existing_rules):
        if part_counter.get(r, 0) == 1 and r not in all_rules:
            to_remove.append(r)

    if to_remove:
        for r in to_remove:
            print(f"❌ 删除规则 {r}（write_counter=1 且不在 all_rules）")
            existing_rules.discard(r)
            part_counter.pop(r, None)

    # write_counter <= 0 -> 写入 retry_rules.txt 并删除
    to_retry = [r for r in existing_rules if part_counter.get(r, 0) <= 0]
    if to_retry:
        # 去重追加
        existing_retry = set()
        if os.path.exists(RETRY_FILE):
            with open(RETRY_FILE, "r", encoding="utf-8") as rf:
                existing_retry = set([l.strip() for l in rf if l.strip()])
        new_retry = [r for r in to_retry if r not in existing_retry]
        if new_retry:
            with open(RETRY_FILE, "a", encoding="utf-8") as rf:
                rf.write("\n".join(new_retry) + "\n")
        print(f"🔥 {len(to_retry)} 条 write_counter<=0 的规则写入 retry_rules.txt（新增 {len(new_retry)} 条）")

        for r in to_retry:
            existing_rules.discard(r)
            part_counter.pop(r, None)

    # 写回 validated_part 文件（合并旧的生效规则与新生效规则）
    final_rules = sorted(existing_rules.union(valid_rules_set))
    with open(validated_file, "w", encoding="utf-8") as f:
        f.write("\n".join(final_rules))

    counter[part_key] = part_counter
    save_bin(NOT_WRITTEN_FILE, counter)

    return len(to_retry)

# ===============================
# 处理分片
# ===============================
def process_part(part):
    part = int(part)
    part_file = os.path.join(TMP_DIR, f"part_{part:02d}.txt")

    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新拉取规则…")
        download_all_sources()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    # 读取当前分片规则
    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").read().splitlines() if l.strip()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    # 每次 process_part 都重新下载最新规则源
    all_rules = []
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]

    for url in urls:
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            all_rules.extend([line.strip() for line in r.text.splitlines() if line.strip()])
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")
    all_rules_set = set(all_rules)

    # delete_counter 处理
    delete_counter = load_bin(DELETE_COUNTER_FILE)
    rules_to_validate = [r for r in lines if int(delete_counter.get(r, 4)) < 7]

    for r in lines:
        cnt = int(delete_counter.get(r, 4))
        if cnt >= 7:
            delete_counter[r] = cnt + 1
        if cnt >= 28 and r not in all_rules_set:
            # 删除过期规则
            delete_counter.pop(r, None)
            if r in lines:
                lines.remove(r)

    # 插入 retry_rules 顶部
    retry_rules = []
    if os.path.exists(RETRY_FILE):
        with open(RETRY_FILE, "r", encoding="utf-8") as rf:
            retry_rules = [r.strip() for r in rf if r.strip()]
        if retry_rules:
            print(f"🔁 将 {len(retry_rules)} 条 retry_rules 插入分片顶部")
            for r in reversed(retry_rules):
                cnt = int(delete_counter.get(r, 4))
                if cnt < 7 and r not in rules_to_validate:
                    rules_to_validate.insert(0, r)
            open(RETRY_FILE, "w", encoding="utf-8").truncate(0)

    # DNS 验证
    valid_rules = set(dns_validate(rules_to_validate, part))
    added_count = len(valid_rules)

    # 读取原有 validated_part_X.txt
    validated_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    existing_rules = set()
    if os.path.exists(validated_file):
        with open(validated_file, "r", encoding="utf-8") as f:
            existing_rules = set(l.strip() for l in f if l.strip())

    # 更新 not_written_counter (write_counter)
    counter = load_bin(NOT_WRITTEN_FILE)
    part_key = f"validated_part_{part}"
    part_counter = counter.get(part_key, {})
    counter.setdefault(part_key, part_counter)

    # DNS 验证成功 → write_counter 重置
    for r in valid_rules:
        part_counter[r] = WRITE_COUNTER_MAX

    # 原有规则未验证成功 → write_counter 递减
    for r in existing_rules - valid_rules:
        part_counter[r] = max(part_counter.get(r, WRITE_COUNTER_MAX) - 1, 0)

    # write_counter <=0 写入 retry_rules.txt 并移除
    to_retry = [r for r in existing_rules.union(valid_rules) if part_counter.get(r, 0) <= 0]
    if to_retry:
        existing_retry = set()
        if os.path.exists(RETRY_FILE):
            with open(RETRY_FILE, "r", encoding="utf-8") as rf:
                existing_retry = set(l.strip() for l in rf if l.strip())
        new_retry = [r for r in to_retry if r not in existing_retry]
        if new_retry:
            with open(RETRY_FILE, "a", encoding="utf-8") as rf:
                rf.write("\n".join(new_retry) + "\n")
        print(f"🔥 {len(to_retry)} 条 write_counter<=0 的规则写入 retry_rules.txt（新增 {len(new_retry)} 条）")
        for r in to_retry:
            part_counter.pop(r, None)
            existing_rules.discard(r)
            valid_rules.discard(r)

    # 最终规则 = 原有规则 + 本次验证成功 - write_counter<=0
    final_rules = sorted(existing_rules.union(valid_rules))

    # 写回 validated_part_X.txt
    with open(validated_file, "w", encoding="utf-8") as f:
        f.write("\n".join(final_rules))

    # 保存 not_written_counter
    counter[part_key] = part_counter
    save_bin(NOT_WRITTEN_FILE, counter)

    # ===== 打印统计 =====
    failure_counts = {}
    for v in part_counter.values():
        v = int(v)
        if 1 <= v <= WRITE_COUNTER_MAX:
            failure_counts[v] = failure_counts.get(v, 0) + 1

    print("\n📊 当前分片连续失败统计:")
    for i in range(1, WRITE_COUNTER_MAX + 1):
        if failure_counts.get(i, 0) > 0:
            print(f"    ⚠ 连续失败 {i}/{WRITE_COUNTER_MAX} 的规则条数: {failure_counts[i]}")

    counts = {i: 0 for i in range(1, WRITE_COUNTER_MAX + 1)}
    for v in part_counter.values():
        if 1 <= v <= WRITE_COUNTER_MAX:
            counts[v] += 1
    total_rules = sum(counts.values())
    print(f"\n📊 当前分片 write_counter 规则统计: 总规则条数 {total_rules}")
    for i in range(1, WRITE_COUNTER_MAX + 1):
        if counts[i] > 0:
            print(f"    ⚠ write_counter {i}/{WRITE_COUNTER_MAX} 的规则条数: {counts[i]}")

    delete_counts = {}
    for r in final_rules:
        cnt = int(delete_counter.get(r, 4))
        delete_counts[cnt] = delete_counts.get(cnt, 0) + 1
    print("\n📊 当前分片 delete_counter 统计:")
    for i in sorted(delete_counts):
        print(f"    ⚠ delete_counter={i} 的规则条数: {delete_counts[i]}")

    print("--------------------------------------------------")
    print(f"✅ 分片 {part} 更新完成: 总 {len(final_rules)}, DNS 验证成功 {added_count}, write_counter<=0 移除 {len(to_retry)}")
    print(f"COMMIT_STATS: 总 {len(final_rules)}, 新增 {added_count}, 删除 {len(to_retry)}, 过滤 {len(rules_to_validate) - added_count}")



# ===============================
# 主入口
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则源并切片")
    args = parser.parse_args()

    if args.force_update or not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()

    if args.part:
        process_part(args.part)
    else:
        print("提示: 使用 --part 指定要验证的分片（1~16）")
