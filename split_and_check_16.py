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

# ===============================
# 配置区
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
# 二进制读取/写入 (msgpack)
# ===============================
def load_bin(path):
    if os.path.exists(path):
        try:
            raw = open(path, "rb").read()
            if not raw:
                return {}
            return msgpack.unpackb(raw, raw=False)
        except Exception as e:
            print(f"⚠ 读取 {path} 错误: {e}")
            return {}
    return {}

def save_bin(path, data):
    try:
        with open(path, "wb") as f:
            f.write(msgpack.packb(data, use_bin_type=True))
    except Exception as e:
        print(f"⚠ 保存 {path} 错误: {e}")

# ===============================
# 文件初始化
# ===============================
for f in [DELETE_COUNTER_FILE, NOT_WRITTEN_FILE]:
    if not os.path.exists(f):
        save_bin(f, {})

if not os.path.exists(RETRY_FILE):
    open(RETRY_FILE, "w", encoding="utf-8").close()

# ===============================
# DNS 验证单条规则
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
# 下载并生成合并规则及分片
# ===============================
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return []

    all_rules = []
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]

    for url in urls:
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            new_rules = [line.strip() for line in r.text.splitlines() if line.strip()]
            all_rules.extend(new_rules)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")

    all_rules = sorted(set(all_rules))
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(all_rules))
    print(f"✅ 合并规则写入 {MASTER_RULE}，共 {len(all_rules)} 条")

    # 分片
    os.makedirs(TMP_DIR, exist_ok=True)
    part_size = (len(all_rules) + PARTS - 1) // PARTS
    for i in range(PARTS):
        part_rules = all_rules[i*part_size:(i+1)*part_size]
        part_file = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"⏱ 分片 {i+1} 写入 {len(part_rules)} 条规则 -> {part_file}")

    return all_rules

# ===============================
# DNS 验证分片
# ===============================
def dns_validate(rules, part):
    valid_rules = []
    total_rules = len(rules)
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=DNS_THREADS) as executor:
        futures = {executor.submit(check_domain, r): r for r in rules}
        completed = 0
        for future in as_completed(futures):
            res = None
            try:
                res = future.result()
            except Exception:
                pass
            if res:
                valid_rules.append(res)
            completed += 1
            if completed % DNS_BATCH_SIZE == 0 or completed == total_rules:
                elapsed = time.time() - start_time
                speed = completed / elapsed if elapsed else 0
                eta = (total_rules - completed) / speed if speed else 0
                print(f"✅ 已验证 {completed}/{total_rules} 条 | 有效 {len(valid_rules)} 条 | 速度 {speed:.1f}/秒 | 预计完成 {eta:.1f}s")
    return valid_rules

# ===============================
# 更新 not_written_counter
# ===============================
def update_not_written_counter(part_num, valid_rules, all_rules):
    part_key = f"validated_part_{part_num}"
    counter = load_bin(NOT_WRITTEN_FILE)
    counter.setdefault(part_key, {})
    validated_file = os.path.join(DIST_DIR, f"{part_key}.txt")
    existing_rules = set(open(validated_file, "r", encoding="utf-8").read().splitlines()) if os.path.exists(validated_file) else set()
    all_rules = set(all_rules)
    part_counter = counter.get(part_key, {})

    for r in valid_rules:
        part_counter[r] = WRITE_COUNTER_MAX

    valid_rules_set = set(valid_rules)
    for r in existing_rules - valid_rules_set:
        part_counter[r] = max(part_counter.get(r, WRITE_COUNTER_MAX) - 1, 0)

    to_remove = [r for r in existing_rules if part_counter.get(r, 0) == 1 and r not in all_rules]
    for r in to_remove:
        existing_rules.discard(r)
        part_counter.pop(r, None)

    to_retry = [r for r in existing_rules if part_counter.get(r, 0) <= 0]
    if to_retry:
        existing_retry = set(open(RETRY_FILE, "r", encoding="utf-8").read().splitlines()) if os.path.exists(RETRY_FILE) else set()
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
# process_part
# ===============================
def process_part(part, all_rules, hash_list, delete_counter):
    part = int(part)
    part_file = os.path.join(TMP_DIR, f"part_{part:02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，自动拉取")
        all_rules = download_all_sources()

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").read().splitlines() if l.strip()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    # 更新 hash_list
    for r in lines:
        if r not in hash_list:
            hash_list[r] = int(hashlib.sha256(r.encode("utf-8")).hexdigest(), 16)
    save_bin(HASH_LIST_FILE, hash_list)

    rules_to_validate = [r for r in lines if int(delete_counter.get(r, 4)) < 7]
    for r in lines:
        if int(delete_counter.get(r, 4)) >= 7:
            delete_counter[r] += 1

    retry_rules = set(open(RETRY_FILE, "r", encoding="utf-8").read().splitlines()) if os.path.exists(RETRY_FILE) else set()
    rules_to_validate = list(retry_rules) + [r for r in rules_to_validate if r not in retry_rules]
    open(RETRY_FILE, "w", encoding="utf-8").truncate(0)

    valid = set(dns_validate(rules_to_validate, part))
    added_count = 0
    new_retry_rules = []

    for r in rules_to_validate:
        if r in valid:
            delete_counter[r] = 0
            added_count += 1
        else:
            delete_counter[r] = int(delete_counter.get(r, 0)) + 1
            if delete_counter[r] <= 0:
                new_retry_rules.append(r)

    if new_retry_rules:
        with open(RETRY_FILE, "a", encoding="utf-8") as rf:
            rf.write("\n".join(new_retry_rules) + "\n")

    save_bin(DELETE_COUNTER_FILE, delete_counter)
    deleted_validated = update_not_written_counter(part, list(valid), all_rules)
    total_count = len(valid)

    print(f"✅ 分片 {part} 完成: 总{total_count}, 新增{added_count}, 删除{deleted_validated}, 过滤{len(rules_to_validate) - len(valid)}")

# ===============================
# 主入口
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则源并切片")
    args = parser.parse_args()

    # 初始化 hash_list
    hash_list = load_bin(HASH_LIST_FILE)
    if not isinstance(hash_list, dict):
        hash_list = {}
        save_bin(HASH_LIST_FILE, hash_list)

    # 初始化 delete_counter
    delete_counter = load_bin(DELETE_COUNTER_FILE)
    if not isinstance(delete_counter, dict):
        delete_counter = {}
        save_bin(DELETE_COUNTER_FILE, delete_counter)

    # 下载规则源 / 分片
    if args.force_update or not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        all_rules = download_all_sources()
    else:
        with open(MASTER_RULE, "r", encoding="utf-8") as f:
            all_rules = [l.strip() for l in f if l.strip()]

    # 执行分片验证
    if args.part:
        process_part(int(args.part), all_rules, hash_list, delete_counter)
    else:
        print("提示: 使用 --part 指定要验证的分片（1~16）")
