#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import msgpack
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

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
WRITE_COUNTER_MAX = 6
DNS_BATCH_SIZE = 540
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
def load_bin(path):
    if os.path.exists(path):
        try:
            with open(path, "rb") as f:
                raw = f.read()
                if not raw:
                    return {}
                return msgpack.unpackb(raw, raw=False)
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
    下载并合并规则源，返回合并后的 all_rules 列表（去重，保留原始字符串格式）。
    同时按你的 delete_counter 规则更新并保存 delete_counter.bin，
    并调用 split_parts 切分需要验证的规则到 TMP_DIR/part_XX.txt。
    """
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return []

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
    # 保持原有顺序但去重（用 dict 保序去重）
    seen = {}
    for r in all_rules:
        if r not in seen:
            seen[r] = True
    all_rules = list(seen.keys())
    all_rules_set = set(all_rules)

    # 载入 delete_counter
    delete_counter = load_bin(DELETE_COUNTER_FILE)

    rules_to_validate = set()
    reset_rules = set()
    removed_rules = set()
    skipped_rules = []

    updated_delete_counter = {}

    # ===== 处理旧规则 =====
    for rule, cnt in delete_counter.items():
        cnt = int(cnt)

        # delete_counter >=118 且不在源 → 删除
        if cnt >= 118 and rule not in all_rules_set:
            removed_rules.add(rule)
            continue

        # delete_counter >=114 且在源 → 重置为 80
        if cnt >= 114 and rule in all_rules_set:
            cnt = 80
            reset_rules.add(rule)

        # 合并后统一 +1
        cnt += 1

        # delete_counter >=97 → 不参与验证
        if cnt >= 97:
            skipped_rules.append(rule)
        else:
            rules_to_validate.add(rule)

        updated_delete_counter[rule] = cnt

    # ===== 处理新规则 =====
    for rule in all_rules:
        if rule not in updated_delete_counter:
            updated_delete_counter[rule] = 64
            rules_to_validate.add(rule)

    # 保存 delete_counter
    save_bin(DELETE_COUNTER_FILE, updated_delete_counter)

    # ===== 输出信息 =====
    if reset_rules:
        for rule in list(reset_rules)[:20]:
            print(f"🔁 删除计数达到114，重置为 80：{rule}")
        print(f"🔢 共 {len(reset_rules)} 条规则 delete_counter≥114，已重置为 80")

    if removed_rules:
        for rule in list(removed_rules)[:20]:
            print(f"🚮 删除计数达到118，移除规则：{rule}")
        print(f"🗑️ 共 {len(removed_rules)} 条规则 delete_counter≥118 且不在源文件，已移除")

    if skipped_rules:
        for rule in list(skipped_rules)[:20]:
            print(f"⏭ 删除计数≥97，跳过验证：{rule}")
        print(f"⏩ 共 {len(skipped_rules)} 条规则 delete_counter≥97 被跳过验证")

    print(
        f"📚 合并总规则 {len(all_rules)} 条，"
        f"⏩ 跳过 {len(skipped_rules)} 条（delete_counter≥97），"
        f"🧮 需要验证 {len(rules_to_validate)} 条（delete_counter<97），"
        f"🪓 即将切分为 {PARTS} 片"
    )

    # 切分进入验证的规则
    split_parts(list(rules_to_validate), updated_delete_counter)

    # 返回内存中的合并规则列表，供 process_part 使用（不写 all_rules.txt）
    return all_rules

# ===============================
# 分片 + 负载均衡
# ===============================
def split_parts(all_rules, delete_counter):
    part_existing = {}
    for i in range(1, PARTS + 1):
        f = os.path.join(DIST_DIR, f"validated_part_{i}.txt")
        if os.path.exists(f):
            with open(f, "r", encoding="utf-8") as ff:
                part_existing[i] = set(l.strip() for l in ff if l.strip())
        else:
            part_existing[i] = set()

    filtered_rules = [r for r in all_rules if int(delete_counter.get(r, 4)) < 97]

    rule_to_part = {}
    for r in filtered_rules:
        assigned = False
        for i in range(1, PARTS + 1):
            if r in part_existing[i]:
                rule_to_part[r] = i
                assigned = True
                break
        if not assigned:
            rule_to_part[r] = None

    part_buckets = {i: [] for i in range(1, PARTS + 1)}

    for r, p in rule_to_part.items():
        if p:
            part_buckets[p].append((r, int(delete_counter.get(r, 4))))

    new_rules = [(r, int(delete_counter.get(r, 4))) for r, p in rule_to_part.items() if p is None]
    new_rules.sort(key=lambda x: x[1])
    for r, cnt in new_rules:
        target = min(part_buckets.items(), key=lambda x: len(x[1]))[0]
        part_buckets[target].append((r, cnt))

    max_iterations = 5
    for _ in range(max_iterations):
        counts = [len(part_buckets[i]) for i in range(1, PARTS + 1)]
        avg = sum(counts) / PARTS
        moved = False
        for i in range(1, PARTS + 1):
            bucket = part_buckets[i]
            bucket_sorted = sorted(bucket, key=lambda x: x[1], reverse=True)
            for r, cnt in bucket_sorted:
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
# 处理分片（现在接收可选的 all_rules）
# ===============================
def process_part(part, all_rules=None):
    """
    part: 分片编号 1..PARTS
    all_rules: 可选，download_all_sources 返回的合并规则列表（内存），若为 None 则内部调用 download_all_sources 获取
    """
    part = int(part)
    part_file = os.path.join(TMP_DIR, f"part_{part:02d}.txt")

    # 如果分片文件缺失，先下载并切片（download_all_sources 会生成分片）
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新拉取规则…")
        all_rules = download_all_sources()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    # 如果调用时没有传入 all_rules，则尝试调用 download_all_sources 获取（不会生成文件，只返回合并列表）
    if all_rules is None:
        all_rules = download_all_sources()

    # 确保 all_rules 为列表
    if not isinstance(all_rules, (list, tuple, set)):
        all_rules = []

    all_rules_set = set(all_rules)

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").read().splitlines() if l.strip()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    delete_counter = load_bin(DELETE_COUNTER_FILE)

    rules_to_validate = list(lines)
    if os.path.exists(RETRY_FILE):
        with open(RETRY_FILE, "r", encoding="utf-8") as rf:
            retry_rules = [r.strip() for r in rf if r.strip()]
        if retry_rules:
            print(f"🔁 将 {len(retry_rules)} 条 retry_rules 插入分片顶部")
            for r in reversed(retry_rules):
                if r not in rules_to_validate:
                    rules_to_validate.insert(0, r)
            # 这里我们仍选择清空 retry 文件（分片执行期间，retry 已被插入）
            open(RETRY_FILE, "w", encoding="utf-8").truncate(0)

    valid_rules = set(dns_validate(rules_to_validate, part))
    added_count = len(valid_rules)

    validated_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    existing_rules = set()
    if os.path.exists(validated_file):
        with open(validated_file, "r", encoding="utf-8") as f:
            existing_rules = set(l.strip() for l in f if l.strip())

    # ===== delete_counter 更新逻辑 =====
    # DNS 成功规则 → 0
    for r in valid_rules:
        delete_counter[r] = 0

    # 其他规则 delete_counter +1
    for r in lines:
        if r not in valid_rules:
            delete_counter[r] = int(delete_counter.get(r, 0)) + 1

    # 写回 delete_counter
    save_bin(DELETE_COUNTER_FILE, delete_counter)

    # ===== write_counter / retry_rules 逻辑保持原样 =====
    counter = load_bin(NOT_WRITTEN_FILE)
    part_key = f"validated_part_{part}"
    part_counter = counter.get(part_key, {})
    counter.setdefault(part_key, part_counter)

    # DNS 成功 → write_counter 重置
    for r in valid_rules:
        part_counter[r] = WRITE_COUNTER_MAX

    # 当前分片已有 write_counter，但不在 DNS 成功 → -1
    for r in existing_rules - valid_rules:
        part_counter[r] = max(part_counter.get(r, WRITE_COUNTER_MAX) - 1, 0)

    # write_counter <=1 且不在 all_rules → 删除
    to_delete = [r for r in existing_rules if part_counter.get(r, 0) <= 1 and r not in all_rules_set]
    for r in to_delete:
        existing_rules.discard(r)
        part_counter.pop(r, None)
        print(f"❌ 删除规则 {r}（write_counter<=1 且不在 all_rules）")

    # write_counter <=0 → retry_rules
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
            existing_rules.discard(r)
            part_counter.pop(r, None)
            valid_rules.discard(r)

    final_rules = sorted(existing_rules.union(valid_rules))
    with open(validated_file, "w", encoding="utf-8") as f:
        f.write("\n".join(final_rules))

    counter[part_key] = part_counter
    save_bin(NOT_WRITTEN_FILE, counter)

    print(f"✅ 分片 {part} 更新完成: 总 {len(final_rules)}, DNS 成功 {added_count}, write_counter<=0 移除 {len(to_retry)}")
    print(f"COMMIT_STATS: 总 {len(final_rules)}, 新增 {added_count}, 删除 {len(to_retry)}, 过滤 {len(rules_to_validate) - added_count}")

# ===============================
# 主入口
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则源并切片")
    args = parser.parse_args()

    # 如果缺少分片或用户要求强制更新，先下载合并并生成分片（download_all_sources 返回合并后的规则列表）
    all_rules = None
    if args.force_update or not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        all_rules = download_all_sources()

    if args.part:
        # 传入 all_rules（如果有），否则 process_part 内部会再次调用 download_all_sources 获取
        process_part(args.part, all_rules=all_rules)
    else:
        print("提示: 使用 --part 指定要验证的分片（1~16）")
