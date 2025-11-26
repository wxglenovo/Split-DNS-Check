#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import msgpack
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed  # ✅ 这里一定要有
import time
import hashlib
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
HASH_LIST_FILE = os.path.join(DIST_DIR, "hash_list.bin")
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.bin")
NOT_WRITTEN_FILE = os.path.join(DIST_DIR, "not_written_counter.bin")
RETRY_FILE = os.path.join(DIST_DIR, "retry_rules.txt")
DELETE_THRESHOLD = 4
DNS_BATCH_SIZE = 540
WRITE_COUNTER_MAX = 6
DNS_THREADS = 80
BALANCE_THRESHOLD = 1
BALANCE_MOVE_LIMIT = 50

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
    下载所有规则源，合并规则，过滤并更新删除计数
    逻辑：
      - 新规则 delete_counter = 4
      - 已有规则但未出现 delete_counter +1
      - delete_counter >=7 → 过滤掉，不进入验证
      - 在 all_rules 中且 delete_counter>=24 → 重置为6
      - 不在 all_rules 中且 delete_counter>=28 → 删除记录
    注意：**不在此处处理 retry_rules.txt**，重试文件由 process_part() 负责在执行分片时加入验证。
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
    delete_counter = load_bin(DELETE_COUNTER_FILE) if os.path.exists(DELETE_COUNTER_FILE) else {}
    updated_delete_counter = {}
    filtered_rules = []
    skipped_count = 0
    reset_rules = []
    removed_rules = []

    # 处理已有 delete_counter 记录
    for rule, cnt in delete_counter.items():
        cnt = int(cnt)
        if rule in all_rules_set:
            if cnt >= 24:
                cnt = WRITE_COUNTER_MAX
                reset_rules.append(rule)
            updated_delete_counter[rule] = cnt
            if cnt < 7:
                filtered_rules.append(rule)
            else:
                skipped_count += 1
        else:
            cnt += 1
            if cnt >= 28:
                removed_rules.append(rule)
                continue
            updated_delete_counter[rule] = cnt
            if cnt < 7:
                filtered_rules.append(rule)
            else:
                skipped_count += 1

    # 处理 all_rules 中的新规则
    for rule in all_rules:
        if rule not in updated_delete_counter:
            updated_delete_counter[rule] = 4
            filtered_rules.append(rule)

    # 保存 delete_counter
    save_bin(DELETE_COUNTER_FILE, updated_delete_counter)

    # 输出重置计数的规则
    if reset_rules:
        for rule in reset_rules[:20]:  # 输出前 20 条规则
            print(f"🔁 删除计数达到24，重置为 6：{rule}")
        print(f"🔢 共 {len(reset_rules)} 条规则的删除计数达到24，已重置为 6")

    if removed_rules:
        print(f"🗑️ 共 {len(removed_rules)} 条规则 delete_counter≥28，已移除")

    if skipped_count > 0:
        skipped_rules = [r for r, cnt in updated_delete_counter.items() if int(cnt) >= 7]
        for rule in skipped_rules[:20]:  # 输出前 20 条被跳过的规则
            print(f"⚠ 删除计数 ≥7，跳过验证：{rule}")
        print(f"🔢 共 {len(skipped_rules)} 条规则被跳过验证（删除计数≥7）")

    print(
        f"📚 合并总规则 {len(all_rules)} 条，"
        f"⏩共 {skipped_count} 条规则被跳过验证，"
        f"🧮 需要验证 {len(filtered_rules)} 条规则，"
        f"🪓 即将切分为 {PARTS} 片"
    )

    # 最终切分（**不包含 retry_rules.txt 的处理**）
    split_parts(filtered_rules, updated_delete_counter)
    return True

# ===============================
# 哈希分片 + 负载均衡优化
# ===============================
def save_hash_list(hashes, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    try:
        with open(filename, "wb") as f:
            pickle.dump(hashes, f)
    except Exception as e:
        print(f"⚠ 保存哈希列表失败: {e}")

def load_hash_list(filename):
    if os.path.exists(filename):
        try:
            with open(filename, "rb") as f:
                data = pickle.load(f)
                if isinstance(data, dict):
                    return data
        except Exception as e:
            print(f"⚠ 加载哈希列表失败: {e}")
    return {}

def split_parts(filtered_rules, delete_counter):
    hash_list = load_hash_list(HASH_LIST_FILE)
    if not isinstance(hash_list, dict):
        hash_list = {}

    for rule in filtered_rules:
        if rule not in hash_list:
            h = int(hashlib.sha256(rule.encode("utf-8")).hexdigest(), 16)
            hash_list[rule] = h

    sorted_rules = sorted(filtered_rules, key=lambda r: hash_list.get(r, 0))

    part_buckets = [[] for _ in range(PARTS)]
    for idx, rule in enumerate(sorted_rules):
        part_idx = idx % PARTS
        part_buckets[part_idx].append(rule)

    os.makedirs(TMP_DIR, exist_ok=True)
    for i, bucket in enumerate(part_buckets):
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(bucket))
        print(f"📄 分片 {i+1}: {len(bucket)} 条规则 → {filename}")

    save_hash_list(hash_list, HASH_LIST_FILE)

# 负载均衡辅助函数（保留原样）
def find_lowest_part(part_buckets):
    lens = [len(b) for b in part_buckets]
    return lens.index(min(lens))

def balance_parts(part_buckets):
    avg_len = sum(len(b) for b in part_buckets) // len(part_buckets)
    for i, bucket in enumerate(part_buckets):
        while len(bucket) > avg_len * 1.2:
            rule = bucket.pop()
            target = find_lowest_part(part_buckets)
            part_buckets[target].append(rule)
    return part_buckets

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
def process_part(part, all_rules, hash_list, delete_counter):
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

    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    old_rules = set(open(out_file, "r", encoding="utf-8").read().splitlines()) if os.path.exists(out_file) else set()

    # delete_counter 来源于传入参数，不再 load_bin()
    rules_to_validate = [r for r in lines if int(delete_counter.get(r, 4)) < 7]

    # delete_counter >= 7 的规则计数继续增长
    for r in lines:
        if int(delete_counter.get(r, 4)) >= 7:
            delete_counter[r] = int(delete_counter.get(r, 4)) + 1

    # retry_rules 前置插入
    retry_rules = []
    if os.path.exists(RETRY_FILE):
        with open(RETRY_FILE, "r", encoding="utf-8") as rf:
            retry_rules = [r.strip() for r in rf if r.strip()]
        if retry_rules:
            print(f"🔁 将 {len(retry_rules)} 条 retry_rules 插入分片顶部")
            for r in reversed(retry_rules):
                if int(delete_counter.get(r, 4)) < 7 and r not in rules_to_validate:
                    rules_to_validate.insert(0, r)
            open(RETRY_FILE, "w", encoding="utf-8").truncate(0)

    # DNS 验证
    valid = set(dns_validate(rules_to_validate, part))
    added_count = 0
    failure_counts = {}
    new_retry_rules = []

    # 更新 delete_counter / retry_rules / 剔除失败规则
    for r in rules_to_validate:
        if r in valid:
            delete_counter[r] = 0
            added_count += 1
        else:
            delete_counter[r] = int(delete_counter.get(r, 0)) + 1
            fc = min(int(delete_counter[r]), WRITE_COUNTER_MAX)
            failure_counts[fc] = failure_counts.get(fc, 0) + 1

            if delete_counter[r] >= DELETE_THRESHOLD and r in valid:
                valid.discard(r)

            # write_counter <= 0 → 加入 retry_rules
            if delete_counter[r] <= 0:
                new_retry_rules.append(r)

    # 写入 retry_rules
    if new_retry_rules:
        with open(RETRY_FILE, "a", encoding="utf-8") as rf:
            for r in new_retry_rules:
                rf.write(r + "\n")
        print(f"🔥 {len(new_retry_rules)} 条 write_counter<=0 的规则写入 retry_rules.txt（新增 {len(new_retry_rules)} 条）")

    # 保存 delete_counter
    save_bin(DELETE_COUNTER_FILE, delete_counter)

    # 使用传入的 all_rules（不再重新下载）
    deleted_validated = update_not_written_counter(part, list(valid), all_rules)
    total_count = len(valid)

    # 连续失败统计
    print("\n📊 当前分片连续失败统计:")
    for i in range(1, WRITE_COUNTER_MAX + 1):
        if failure_counts.get(i, 0) > 0:
            print(f"    ⚠ 连续失败 {i}/{WRITE_COUNTER_MAX} 的规则条数: {failure_counts[i]}")

    # write_counter 统计
    print("\n📊 当前分片 write_counter 规则统计:")
    part_key = f"validated_part_{part}"
    counter = load_bin(NOT_WRITTEN_FILE)
    part_counter = counter.get(part_key, {})
    counts = {i: 0 for i in range(1, WRITE_COUNTER_MAX + 1)}
    for v in part_counter.values():
        v = int(v)
        if 1 <= v <= WRITE_COUNTER_MAX:
            counts[v] += 1

    total_rules = sum(counts.values())
    print(f"    ℹ️ 总规则条数: {total_rules}")
    for i in range(1, WRITE_COUNTER_MAX + 1):
        if counts[i] > 0:
            print(f"    ⚠ write_counter {i}/{WRITE_COUNTER_MAX} 的规则条数: {counts[i]}")

    print("--------------------------------------------------")

    # 保存验证成功规则，不加 write_counter
    with open(out_file, "w", encoding="utf-8") as f:
        for r in sorted(valid):
            f.write(r + "\n")

    print(f"✅ 分片 {part} 完成: 总{total_count}, 新增{added_count}, 删除{deleted_validated}, "
          f"过滤{len(rules_to_validate) - len(valid)}")
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {deleted_validated}, "
          f"过滤 {len(rules_to_validate) - len(valid)}")


# ===============================
# 主入口
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则源并切片")
    args = parser.parse_args()

    # 载入 hash_list
    try:
        hash_list = load_bin(HASH_LIST_FILE)
        if not isinstance(hash_list, dict):
            raise Exception()
    except:
        print("⚠ hash_list.bin 损坏，重建空表")
        hash_list = {}
        save_bin(HASH_LIST_FILE, hash_list)

    # 载入 delete_counter
    try:
        delete_counter = load_bin(DELETE_COUNTER_FILE)
        if not isinstance(delete_counter, dict):
            raise Exception()
    except:
        print("⚠ delete_counter.bin 损坏，重建空表")
        delete_counter = {}
        save_bin(DELETE_COUNTER_FILE, delete_counter)

    # 全量规则 all_rules
    all_rules = load_all_remote_rules()

    if args.force_update or not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()
        all_rules = load_all_remote_rules()

    if args.part:
        process_part(int(args.part), all_rules, hash_list, delete_counter)
    else:
        print("提示: 使用 --part 指定要验证的分片（1~16）")
