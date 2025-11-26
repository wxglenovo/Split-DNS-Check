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
DNS_BATCH_SIZE = 540
WRITE_COUNTER_MAX = 6
DNS_THREADS = 80

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 初始化文件
# ===============================
def init_files():
    for f in [DELETE_COUNTER_FILE, NOT_WRITTEN_FILE]:
        if not os.path.exists(f):
            with open(f, "wb") as fp:
                fp.write(msgpack.packb({}, use_bin_type=True))
            print(f"✅ 已创建 {f}")
        else:
            print(f"ℹ️ {f} 已存在")
    
    if not os.path.exists(RETRY_FILE):
        open(RETRY_FILE, "w", encoding="utf-8").close()
        print("✅ 已创建 retry_rules.txt")
    else:
        print("ℹ️ retry_rules.txt 已存在")

    if not os.path.exists(HASH_LIST_FILE):
        # 用 list 存储 hash 值，保证分片兼容
        with open(HASH_LIST_FILE, "wb") as fp:
            pickle.dump([], fp)
        print("✅ 已创建 hash_list.bin")
    else:
        print("ℹ️ hash_list.bin 已存在")

init_files()

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
    return all_rules

# ===============================
# 分片 + 哈希负载均衡
# ===============================
def load_hash_list():
    if os.path.exists(HASH_LIST_FILE):
        with open(HASH_LIST_FILE, "rb") as f:
            return pickle.load(f)
    return []

def save_hash_list(hashes):
    with open(HASH_LIST_FILE, "wb") as f:
        pickle.dump(hashes, f)

def split_parts(all_rules):
    hash_list = load_hash_list()
    hash_dict = {r: h for r, h in hash_list} if hash_list else {}

    # 更新 hash_dict
    for rule in all_rules:
        if rule not in hash_dict:
            h = int(hashlib.sha256(rule.encode("utf-8")).hexdigest(), 16)
            hash_dict[rule] = h

    # 排序 + 分片
    sorted_rules = sorted(all_rules, key=lambda r: hash_dict.get(r, 0))
    part_buckets = [[] for _ in range(PARTS)]
    for idx, rule in enumerate(sorted_rules):
        part_buckets[idx % PARTS].append(rule)

    # 写入分片
    for i, bucket in enumerate(part_buckets):
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(bucket))
        print(f"📄 分片 {i+1}: {len(bucket)} 条规则 → {filename}")

    # 保存 hash_list
    save_hash_list(list(hash_dict.items()))

# ===============================
# DNS 验证 & 更新 not_written_counter
# ===============================
def dns_validate(rules):
    valid_rules = []
    total = len(rules)
    start = time.time()
    with ThreadPoolExecutor(max_workers=DNS_THREADS) as executor:
        futures = {executor.submit(check_domain, r): r for r in rules}
        for i, future in enumerate(as_completed(futures), 1):
            r = future.result()
            if r:
                valid_rules.append(r)
            if i % DNS_BATCH_SIZE == 0 or i == total:
                elapsed = time.time() - start
                speed = i / elapsed if elapsed else 0
                eta = (total - i) / speed if speed else 0
                print(f"✅ 已验证 {i}/{total} 条 | 有效 {len(valid_rules)} 条 | 速度 {speed:.1f}/秒 | 预计完成 {eta:.1f}s")
    return valid_rules

def update_not_written_counter(part, valid_rules, all_rules):
    part_key = f"validated_part_{part}"
    counter = load_bin(NOT_WRITTEN_FILE)
    counter.setdefault(part_key, {})
    part_counter = counter[part_key]

    existing_rules = set()
    validated_file = os.path.join(DIST_DIR, f"{part_key}.txt")
    if os.path.exists(validated_file):
        with open(validated_file, "r", encoding="utf-8") as f:
            existing_rules = set([l.strip() for l in f if l.strip()])

    # 更新 write_counter
    for r in valid_rules:
        part_counter[r] = WRITE_COUNTER_MAX
    for r in existing_rules - set(valid_rules):
        part_counter[r] = max(part_counter.get(r, WRITE_COUNTER_MAX) - 1, 0)

    # 写入 validated_part
    final_rules = sorted(set(valid_rules).union(existing_rules))
    with open(validated_file, "w", encoding="utf-8") as f:
        f.write("\n".join(final_rules))

    counter[part_key] = part_counter
    save_bin(NOT_WRITTEN_FILE, counter)
    return len([r for r in existing_rules if part_counter.get(r,0)==0])

# ===============================
# 分片处理
# ===============================
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，强制下载")
        all_rules = download_all_sources()
        if all_rules:
            split_parts(all_rules)

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").read().splitlines() if l.strip()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    # 加载 delete_counter
    delete_counter = load_bin(DELETE_COUNTER_FILE)
    rules_to_validate = [r for r in lines if int(delete_counter.get(r,4)) < 7]

    # 插入 retry_rules
    if os.path.exists(RETRY_FILE):
        with open(RETRY_FILE, "r", encoding="utf-8") as rf:
            retry_rules = [r.strip() for r in rf if r.strip()]
        if retry_rules:
            print(f"🔁 将 {len(retry_rules)} 条 retry_rules 插入顶部")
            for r in reversed(retry_rules):
                if r not in rules_to_validate:
                    rules_to_validate.insert(0, r)
            open(RETRY_FILE,"w",encoding="utf-8").truncate(0)

    valid_rules = dns_validate(rules_to_validate)

    # 更新 delete_counter
    new_retry = []
    for r in rules_to_validate:
        if r in valid_rules:
            delete_counter[r] = 0
        else:
            delete_counter[r] = int(delete_counter.get(r,0)) + 1
            if delete_counter[r] <= 0:
                new_retry.append(r)
    if new_retry:
        with open(RETRY_FILE,"a",encoding="utf-8") as rf:
            rf.write("\n".join(new_retry)+"\n")
    save_bin(DELETE_COUNTER_FILE, delete_counter)

    # 更新 not_written_counter
    all_rules = download_all_sources() or []
    deleted_count = update_not_written_counter(part, valid_rules, all_rules)

    print(f"✅ 分片 {part} 完成: 有效 {len(valid_rules)}, 删除 {deleted_count}")

# ===============================
# 主入口
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则源并切片")
    args = parser.parse_args()

    if args.force_update or not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR,"part_01.txt")):
        all_rules = download_all_sources()
        if all_rules:
            split_parts(all_rules)

    if args.part:
        process_part(args.part)
    else:
        print("提示: 使用 --part 指定要验证的分片（1~16）")
