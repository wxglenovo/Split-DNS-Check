#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import msgpack
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
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

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 初始化文件
# ===============================
for f in [DELETE_COUNTER_FILE, NOT_WRITTEN_FILE]:
    if not os.path.exists(f):
        with open(f, "wb") as fp:
            msgpack.packb({}, use_bin_type=True)
        print(f"✅ 已创建 {f}")
    else:
        print(f"ℹ️ {f} 已存在")

if not os.path.exists(RETRY_FILE):
    open(RETRY_FILE, "w", encoding="utf-8").close()
    print("✅ 已创建 retry_rules.txt")
else:
    print("ℹ️ retry_rules.txt 已存在")

# 确保 hash_list.bin 文件存在
if not os.path.exists(HASH_LIST_FILE):
    with open(HASH_LIST_FILE, "wb") as fp:
        pickle.dump([], fp)
    print("✅ 已创建 hash_list.bin")
else:
    print("ℹ️ hash_list.bin 已存在")

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
# 打印 not_written_counter 统计
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

    delete_counter = load_bin(DELETE_COUNTER_FILE)
    updated_delete_counter = {}
    filtered_rules = []
    skipped_count = 0
    reset_rules = []
    removed_rules = []

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

    for rule in all_rules:
        if rule not in updated_delete_counter:
            updated_delete_counter[rule] = 4
            filtered_rules.append(rule)

    save_bin(DELETE_COUNTER_FILE, updated_delete_counter)

    split_parts(filtered_rules, updated_delete_counter)
    return True

# ===============================
# 哈希分片 + 负载均衡
# ===============================
def save_hash_list(hashes, filename):
    with open(filename, "wb") as f:
        pickle.dump(hashes, f)

def load_hash_list(filename):
    if os.path.exists(filename):
        with open(filename, "rb") as f:
            data = pickle.load(f)
            if isinstance(data, dict):
                return data
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

# ===============================
# DNS 验证 & not_written_counter 更新
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

def update_not_written_counter(part_num, valid_rules, all_rules):
    part_key = f"validated_part_{part_num}"
    counter = load_bin(NOT_WRITTEN_FILE)

    for i in range(1, PARTS + 1):
        counter.setdefault(f"validated_part_{i}", {})

    validated_file = os.path.join(DIST_DIR, f"{part_key}.txt")
    existing_rules = set(open(validated_file, "r", encoding="utf-8").read().splitlines()) if os.path.exists(validated_file) else set()
    all_rules = set(all_rules)
    part_counter = counter.get(part_key, {})

    for r in valid_rules:
        part_counter[r] = WRITE_COUNTER_MAX

    valid_rules_set = set(valid_rules)

    for r in existing_rules - valid_rules_set:
        part_counter[r] = max(part_counter.get(r, WRITE_COUNTER_MAX) - 1, 0)

    to_remove = [r for r in list(existing_rules) if part_counter.get(r, 0) == 1 and r not in all_rules]
    for r in to_remove:
        existing_rules.discard(r)
        part_counter.pop(r, None)

    to_retry = [r for r in existing_rules if part_counter.get(r, 0) <= 0]
    if to_retry:
        existing_retry = set()
        if os.path.exists(RETRY_FILE):
            with open(RETRY_FILE, "r", encoding="utf-8") as rf:
                existing_retry = set([l.strip() for l in rf if l.strip()])
        new_retry = [r for r in to_retry if r not in existing_retry]
        if new_retry:
            with open(RETRY_FILE, "a", encoding="utf-8") as rf:
                rf.write("\n".join(new_retry) + "\n")

        for r in to_retry:
            existing_rules.discard(r)
            part_counter.pop(r, None)

    final_rules = sorted(existing_rules.union(valid_rules_set))
    with open(validated_file, "w", encoding="utf-8") as f:
        f.write("\n".join(final_rules))

    counter[part_key] = part_counter
    save_bin(NOT_WRITTEN_FILE, counter)

    return len(to_retry)

# ===============================
# 分片处理
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

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").read().splitlines() if l.strip()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    old_rules = set(open(out_file, "r", encoding="utf-8").read().splitlines()) if os.path.exists(out_file) else set()

    delete_counter = load_bin(DELETE_COUNTER_FILE)
    rules_to_validate = [r for r in lines if int(delete_counter.get(r, 4)) < 7]

    for r in lines:
        if int(delete_counter.get(r, 4)) >= 7:
            delete_counter[r] = int(delete_counter.get(r, 4)) + 1

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

    valid = set(dns_validate(rules_to_validate, part))
    added_count = 0
    failure_counts = {}
    new_retry_rules = []

    for r in rules_to_validate:
        if r in valid:
            delete_counter[r] = 0
            added_count += 1
        else:
            delete_counter[r] = int(delete_counter.get(r, 0)) + 1
            fc = min(int(delete_counter[r]), WRITE_COUNTER_MAX)
            failure_counts[fc] = failure_counts.get(fc, 0) + 1
            if delete_counter[r] <= 0:
                new_retry_rules.append(r)

    if new_retry_rules:
        with open(RETRY_FILE, "a", encoding="utf-8") as rf:
            for r in new_retry_rules:
                rf.write(r + "\n")
        print(f"🔥 {len(new_retry_rules)} 条 write_counter<=0 的规则写入 retry_rules.txt（新增 {len(new_retry_rules)} 条）")

    save_bin(DELETE_COUNTER_FILE, delete_counter)

    all_rules = []
    if os.path.exists(URLS_TXT):
        with open(URLS_TXT, "r", encoding="utf-8") as f:
            urls = [u.strip() for u in f if u.strip()]
        for url in urls:
            try:
                r = requests.get(url, timeout=20)
                r.raise_for_status()
                all_rules.extend([line.strip() for line in r.text.splitlines() if line.strip()])
            except Exception as e:
                print(f"⚠ 下载失败 {url}: {e}")

    deleted_validated = update_not_written_counter(part, list(valid), all_rules)
    total_count = len(valid)

    print("\n📊 当前分片连续失败统计:")
    for i in range(1, WRITE_COUNTER_MAX + 1):
        if failure_counts.get(i, 0) > 0:
            print(f"    ⚠ 连续失败 {i}/{WRITE_COUNTER_MAX} 的规则条数: {failure_counts[i]}")

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

    with open(out_file, "w", encoding="utf-8") as f:
        for r in sorted(valid):
            f.write(r + "\n")

    print(f"✅ 分片 {part} 完成: 总{total_count}, 新增{added_count}, 删除{deleted_validated}, 过滤{len(rules_to_validate) - len(valid)}")
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {deleted_validated}, 过滤 {len(rules_to_validate) - len(valid)}")

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
