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
# 二进制读取/写入（msgpack）
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

def save_bin(path, data):
    try:
        with open(path, "wb") as f:
            f.write(msgpack.packb(data, use_bin_type=True))
    except Exception as e:
        print(f"⚠ 保存 {path} 错误: {e}")

# ===============================
# 哈希列表（pickle）
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

# ===============================
# 下载所有规则
# ===============================
def load_all_remote_rules():
    all_rules = []
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return all_rules
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]
    for url in urls:
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            lines = [line.strip() for line in r.text.splitlines() if line.strip()]
            all_rules.extend(lines)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")
    return all_rules

# ===============================
# DNS 验证
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

def dns_validate(rules, part):
    valid_rules = []
    total_rules = len(rules)
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=DNS_THREADS) as executor:
        futures = {executor.submit(check_domain, r): r for r in rules}
        completed = 0
        for future in as_completed(futures):
            res = future.result()
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
    for i in range(1, PARTS+1):
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

    to_remove = [r for r in existing_rules if part_counter.get(r, 0) == 1 and r not in all_rules]
    for r in to_remove:
        existing_rules.discard(r)
        part_counter.pop(r, None)
        print(f"❌ 删除规则 {r}（write_counter=1 且不在 all_rules）")

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
        print(f"🔥 {len(to_retry)} 条 write_counter<=0 的规则写入 retry_rules.txt（新增 {len(new_retry)} 条）")
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
# 分片验证（四参数版）
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

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").read().splitlines() if l.strip()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    # DNS 验证
    rules_to_validate = [r for r in lines if int(delete_counter.get(r, 4)) < 7]
    retry_rules = []
    if os.path.exists(RETRY_FILE):
        with open(RETRY_FILE, "r", encoding="utf-8") as rf:
            retry_rules = [r.strip() for r in rf if r.strip()]
        if retry_rules:
            print(f"🔁 将 {len(retry_rules)} 条 retry_rules 插入分片顶部")
            for r in reversed(retry_rules):
                if r not in rules_to_validate:
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
            if delete_counter[r] >= DELETE_THRESHOLD and r in valid:
                valid.discard(r)

    if new_retry_rules:
        with open(RETRY_FILE, "a", encoding="utf-8") as rf:
            for r in new_retry_rules:
                rf.write(r + "\n")
        print(f"🔥 {len(new_retry_rules)} 条 write_counter<=0 的规则写入 retry_rules.txt（新增 {len(new_retry_rules)} 条）")

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

    # hash_list
    if os.path.exists(HASH_LIST_FILE):
        try:
            hash_list = load_hash_list(HASH_LIST_FILE)
        except:
            print("⚠ hash_list.bin 损坏，重建空表")
            hash_list = {}
            save_hash_list(hash_list, HASH_LIST_FILE)
    else:
        hash_list = {}
        save_hash_list(hash_list, HASH_LIST_FILE)

    # delete_counter
    delete_counter = load_bin(DELETE_COUNTER_FILE)
    if not isinstance(delete_counter, dict):
        print("⚠ delete_counter.bin 损坏，重建空表")
        delete_counter = {}
        save_bin(DELETE_COUNTER_FILE, delete_counter)

    # all_rules
    all_rules = load_all_remote_rules()

    if args.force_update or not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()
        all_rules = load_all_remote_rules()

    if args.part:
        process_part(int(args.part), all_rules, hash_list, delete_counter)
    else:
        print("提示: 使用 --part 指定要验证的分片（1~16）")
