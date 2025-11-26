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
DELETE_THRESHOLD = 4
WRITE_COUNTER_MAX = 6
DNS_THREADS = 80
DNS_BATCH_SIZE = 540

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 文件确保函数（msgpack 空 dict）
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
# 读取 / 写入 bin 文件
# ===============================
def load_bin(path):
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

def save_bin(path, data):
    try:
        with open(path, "wb") as f:
            f.write(msgpack.packb(data, use_bin_type=True))
    except Exception as e:
        print(f"⚠ 保存 {path} 错误: {e}")

# ===============================
# 打印 not_written_counter 概览
# ===============================
def print_not_written_stats():
    data = load_bin(NOT_WRITTEN_FILE)
    counts = {}
    total = 0
    for part_rules in data.values():
        if not isinstance(part_rules, dict):
            continue
        for v in part_rules.values():
            total += 1
            c = min(int(v), WRITE_COUNTER_MAX)
            counts[c] = counts.get(c, 0) + 1
    print(f"📊 not_written_counter 总规则: {total}")
    for k in sorted(counts.keys()):
        print(f"    ⚠ write_counter={k}: {counts[k]} 条")
    return counts

# ===============================
# 单条规则 DNS 验证
# ===============================
def check_domain(rule):
    resolver = dns.resolver.Resolver()
    resolver.timeout = DNS_TIMEOUT
    resolver.lifetime = DNS_TIMEOUT
    domain = rule.lstrip("|").split("^")[0].replace("*","")
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
            new_rules = [l.strip() for l in r.text.splitlines() if l.strip()]
            all_rules.extend(new_rules)
            print(f"🔄 下载 {url} 成功，获取 {len(new_rules)} 条规则")
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")

    print(f"✅ 合并总规则 {len(all_rules)} 条")
    delete_counter = load_bin(DELETE_COUNTER_FILE)
    updated_delete_counter = {}
    filtered_rules = []
    skipped_count = 0

    all_rules_set = set(all_rules)
    reset_rules, removed_rules = [], []

    # 处理 delete_counter
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

    # 新规则
    for rule in all_rules:
        if rule not in updated_delete_counter:
            updated_delete_counter[rule] = 4
            filtered_rules.append(rule)

    save_bin(DELETE_COUNTER_FILE, updated_delete_counter)

    # 输出信息
    if reset_rules:
        print(f"🔁 共 {len(reset_rules)} 条规则删除计数达到24，重置为 6")
    if removed_rules:
        print(f"🗑️ 共 {len(removed_rules)} 条规则 delete_counter≥28，已移除")
    if skipped_count > 0:
        print(f"⚠ 共 {skipped_count} 条规则被跳过验证（delete_counter≥7）")

    # 分片切分
    split_parts(filtered_rules)
    return True

# ===============================
# 哈希分片 + 负载均衡
# ===============================
def save_hash_list(hashes):
    try:
        with open(HASH_LIST_FILE, "wb") as f:
            pickle.dump(hashes, f)
    except Exception as e:
        print(f"⚠ 保存 hash_list 失败: {e}")

def load_hash_list():
    if os.path.exists(HASH_LIST_FILE):
        try:
            with open(HASH_LIST_FILE, "rb") as f:
                data = pickle.load(f)
                if isinstance(data, dict):
                    return data
        except Exception as e:
            print(f"⚠ 加载 hash_list 失败: {e}")
    return {}

def split_parts(filtered_rules):
    hash_list = load_hash_list()
    if not isinstance(hash_list, dict):
        hash_list = {}

    for rule in filtered_rules:
        if rule not in hash_list:
            h = int(hashlib.sha256(rule.encode("utf-8")).hexdigest(),16)
            hash_list[rule] = h

    sorted_rules = sorted(filtered_rules, key=lambda r: hash_list.get(r,0))
    part_buckets = [[] for _ in range(PARTS)]
    for idx, rule in enumerate(sorted_rules):
        part_buckets[idx % PARTS].append(rule)

    for i, bucket in enumerate(part_buckets):
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(bucket))
        print(f"📄 分片 {i+1}: {len(bucket)} 条规则 → {filename}")

    save_hash_list(hash_list)

# ===============================
# DNS 验证
# ===============================
def dns_validate(rules, part):
    valid = []
    total = len(rules)
    start = time.time()
    with ThreadPoolExecutor(max_workers=DNS_THREADS) as executor:
        futures = {executor.submit(check_domain,r):r for r in rules}
        completed = 0
        for future in as_completed(futures):
            res = None
            try:
                res = future.result()
            except Exception:
                pass
            if res:
                valid.append(res)
            completed += 1
            if completed % DNS_BATCH_SIZE == 0 or completed==total:
                elapsed = time.time()-start
                speed = completed/elapsed if elapsed>0 else 0
                eta = (total-completed)/speed if speed>0 else 0
                print(f"✅ 已验证 {completed}/{total} 条 | 有效 {len(valid)} 条 | 速度 {speed:.1f}/秒 | 预计完成 {eta:.1f}s")
    return valid

# ===============================
# 更新 not_written_counter
# ===============================
def update_not_written_counter(part_num, valid_rules, all_rules):
    part_key = f"validated_part_{part_num}"
    counter = load_bin(NOT_WRITTEN_FILE)
    for i in range(1, PARTS+1):
        counter.setdefault(f"validated_part_{i}", {})
    validated_file = os.path.join(DIST_DIR, f"{part_key}.txt")
    existing_rules = set(open(validated_file,"r",encoding="utf-8").read().splitlines()) if os.path.exists(validated_file) else set()
    all_rules = set(all_rules)
    part_counter = counter.get(part_key,{})

    # 新验证成功规则
    for r in valid_rules:
        part_counter[r] = WRITE_COUNTER_MAX

    valid_set = set(valid_rules)
    # 旧规则没出现 -> 递减
    for r in existing_rules - valid_set:
        part_counter[r] = max(part_counter.get(r,WRITE_COUNTER_MAX)-1,0)

    # write_counter<=0 -> retry
    to_retry = [r for r in existing_rules if part_counter.get(r,0)<=0]
    if to_retry:
        existing_retry = set()
        if os.path.exists(RETRY_FILE):
            existing_retry = set([l.strip() for l in open(RETRY_FILE,"r",encoding="utf-8") if l.strip()])
        new_retry = [r for r in to_retry if r not in existing_retry]
        if new_retry:
            with open(RETRY_FILE,"a",encoding="utf-8") as rf:
                rf.write("\n".join(new_retry)+"\n")
        for r in to_retry:
            existing_rules.discard(r)
            part_counter.pop(r,None)
        print(f"🔥 {len(to_retry)} 条 write_counter<=0 规则写入 retry_rules.txt（新增 {len(new_retry)} 条）")

    # 保存 validated 文件
    final_rules = sorted(existing_rules.union(valid_set))
    with open(validated_file,"w",encoding="utf-8") as f:
        f.write("\n".join(final_rules))

    counter[part_key] = part_counter
    save_bin(NOT_WRITTEN_FILE,counter)
    return len(to_retry)

# ===============================
# 处理分片
# ===============================
def process_part(part):
    part = int(part)
    part_file = os.path.join(TMP_DIR, f"part_{part:02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，自动下载")
        download_all_sources()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = [l.strip() for l in open(part_file,"r",encoding="utf-8").read().splitlines() if l.strip()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    delete_counter = load_bin(DELETE_COUNTER_FILE)
    rules_to_validate = [r for r in lines if int(delete_counter.get(r,4))<7]

    # retry_rules 插入顶部
    retry_rules=[]
    if os.path.exists(RETRY_FILE):
        retry_rules=[r.strip() for r in open(RETRY_FILE,"r",encoding="utf-8") if r.strip()]
        if retry_rules:
            print(f"🔁 将 {len(retry_rules)} 条 retry_rules 插入分片顶部")
            for r in reversed(retry_rules):
                if int(delete_counter.get(r,4))<7 and r not in rules_to_validate:
                    rules_to_validate.insert(0,r)
            open(RETRY_FILE,"w",encoding="utf-8").truncate(0)

    # DNS 验证
    valid = set(dns_validate(rules_to_validate, part))
    update_not_written_counter(part, list(valid), lines)

    # 保存最终验证结果
    validated_file = os.path.join(DIST_DIR,f"validated_part_{part}.txt")
    with open(validated_file,"w",encoding="utf-8") as f:
        f.write("\n".join(sorted(valid)))

    print(f"✅ 分片 {part} 验证完成: 有效 {len(valid)} 条")
    print("--------------------------------------------------")

# ===============================
# 主入口
# ===============================
if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="指定分片 1~16")
    parser.add_argument("--force-update",action="store_true",help="强制下载并切片")
    args = parser.parse_args()

    if args.force_update or not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR,"part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()

    if args.part:
        process_part(args.part)
    else:
        print("提示: 使用 --part 指定分片验证（1~16）")
