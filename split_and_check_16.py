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
DELETE_THRESHOLD = 4

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 文件确保函数
# ===============================
def ensure_bin_file(path):
    if not os.path.exists(path):
        with open(path, "wb") as f:
            f.write(msgpack.packb({}, use_bin_type=True))

ensure_bin_file(DELETE_COUNTER_FILE)
ensure_bin_file(NOT_WRITTEN_FILE)
if not os.path.exists(RETRY_FILE):
    open(RETRY_FILE, "w", encoding="utf-8").close()

# ===============================
# 二进制读写
# ===============================
def load_bin(path):
    if os.path.exists(path):
        try:
            with open(path, "rb") as f:
                raw = f.read()
                if not raw:
                    return {}
                return msgpack.unpackb(raw, raw=False)
        except:
            return {}
    return {}

def save_bin(path, data):
    with open(path, "wb") as f:
        f.write(msgpack.packb(data, use_bin_type=True))

# ===============================
# 单条 DNS 验证
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
    except:
        return None

# ===============================
# 下载合并规则
# ===============================
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False
    print("📥 下载规则源...")
    all_rules = []
    with open(URLS_TXT,"r",encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]
    for url in urls:
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            new_rules = [line.strip() for line in r.text.splitlines() if line.strip()]
            all_rules.extend(new_rules)
        except:
            pass
    print(f"✅ 合并 {len(all_rules)} 条规则")
    all_rules_set = set(all_rules)
    delete_counter = load_bin(DELETE_COUNTER_FILE)
    updated_delete_counter = {}
    filtered_rules = []

    for rule, cnt in delete_counter.items():
        cnt = int(cnt)
        if rule in all_rules_set:
            if cnt >= 24:
                cnt = WRITE_COUNTER_MAX
            updated_delete_counter[rule] = cnt
            if cnt < 7:
                filtered_rules.append(rule)
        else:
            cnt += 1
            if cnt >= 28:
                continue
            updated_delete_counter[rule] = cnt
            if cnt < 7:
                filtered_rules.append(rule)

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
def load_hash_list(filename):
    if os.path.exists(filename):
        try:
            data = pickle.load(open(filename,"rb"))
            if isinstance(data, dict):
                return data
        except:
            pass
    return {}

def save_hash_list(hashes, filename):
    pickle.dump(hashes, open(filename,"wb"))

def split_parts(filtered_rules, delete_counter):
    hash_list = load_hash_list(HASH_LIST_FILE)
    for rule in filtered_rules:
        if rule not in hash_list:
            hash_list[rule] = int(hashlib.sha256(rule.encode("utf-8")).hexdigest(),16)
    sorted_rules = sorted(filtered_rules, key=lambda r: hash_list.get(r,0))
    part_buckets = [[] for _ in range(PARTS)]
    for idx, rule in enumerate(sorted_rules):
        part_buckets[idx % PARTS].append(rule)
    for i,bucket in enumerate(part_buckets):
        filename = os.path.join(TMP_DIR,f"part_{i+1:02d}.txt")
        with open(filename,"w",encoding="utf-8") as f:
            f.write("\n".join(bucket))
    save_hash_list(hash_list,HASH_LIST_FILE)

# ===============================
# DNS 验证
# ===============================
def dns_validate(rules, part):
    valid = []
    total = len(rules)
    start = time.time()
    with ThreadPoolExecutor(max_workers=DNS_THREADS) as executor:
        futures = {executor.submit(check_domain,r): r for r in rules}
        completed = 0
        for future in as_completed(futures):
            res = None
            try:
                res = future.result()
            except:
                pass
            if res:
                valid.append(res)
            completed +=1
            if completed%DNS_BATCH_SIZE==0 or completed==total:
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
    for i in range(1,PARTS+1):
        counter.setdefault(f"validated_part_{i}",{})
    validated_file = os.path.join(DIST_DIR,f"{part_key}.txt")
    existing_rules = set(open(validated_file,"r",encoding="utf-8").read().splitlines()) if os.path.exists(validated_file) else set()
    all_rules = set(all_rules)
    part_counter = counter.get(part_key,{})
    for r in valid_rules:
        part_counter[r]=WRITE_COUNTER_MAX
    valid_set = set(valid_rules)
    for r in existing_rules - valid_set:
        part_counter[r]=max(part_counter.get(r,WRITE_COUNTER_MAX)-1,0)
    # write_counter<=0写入retry_rules
    to_retry=[r for r in existing_rules if part_counter.get(r,0)<=0]
    if to_retry:
        existing_retry=set()
        if os.path.exists(RETRY_FILE):
            existing_retry=set([l.strip() for l in open(RETRY_FILE,"r") if l.strip()])
        new_retry=[r for r in to_retry if r not in existing_retry]
        if new_retry:
            with open(RETRY_FILE,"a",encoding="utf-8") as rf:
                rf.write("\n".join(new_retry)+"\n")
        for r in to_retry:
            existing_rules.discard(r)
            part_counter.pop(r,None)
    final_rules = sorted(existing_rules.union(valid_set))
    with open(validated_file,"w",encoding="utf-8") as f:
        f.write("\n".join(final_rules))
    counter[part_key]=part_counter
    save_bin(NOT_WRITTEN_FILE,counter)
    return len(to_retry)

# ===============================
# 处理分片
# ===============================
def process_part(part):
    part=int(part)
    part_file=os.path.join(TMP_DIR,f"part_{part:02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新拉取规则…")
        download_all_sources()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止"); return
    lines=[l.strip() for l in open(part_file,"r",encoding="utf-8").read().splitlines() if l.strip()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")
    out_file=os.path.join(DIST_DIR,f"validated_part_{part}.txt")
    old_rules=set(open(out_file,"r",encoding="utf-8").read().splitlines()) if os.path.exists(out_file) else set()
    delete_counter = load_bin(DELETE_COUNTER_FILE)
    rules_to_validate=[r for r in lines if int(delete_counter.get(r,4))<7]
    # retry_rules加入顶部
    retry=[]
    if os.path.exists(RETRY_FILE):
        retry=[r.strip() for r in open(RETRY_FILE,"r") if r.strip()]
        if retry:
            for r in reversed(retry):
                if int(delete_counter.get(r,4))<7 and r not in rules_to_validate:
                    rules_to_validate.insert(0,r)
            open(RETRY_FILE,"w").truncate(0)
    valid = set(dns_validate(rules_to_validate, part))
    # 更新delete_counter
    for r in rules_to_validate:
        if r in valid:
            delete_counter[r]=0
        else:
            delete_counter[r]=int(delete_counter.get(r,0))+1
    save_bin(DELETE_COUNTER_FILE,delete_counter)
    # 下载all_rules
    all_rules=[]
    if os.path.exists(URLS_TXT):
        with open(URLS_TXT,"r",encoding="utf-8") as f:
            urls=[u.strip() for u in f if u.strip()]
        for url in urls:
            try:
                r=requests.get(url,timeout=20)
                r.raise_for_status()
                all_rules.extend([line.strip() for line in r.text.splitlines() if line.strip()])
            except:
                pass
    deleted_validated = update_not_written_counter(part,list(valid),all_rules)
    total_count = len(valid)
    with open(out_file,"w",encoding="utf-8") as f:
        for r in sorted(valid):
            f.write(r+"\n")
    print(f"✅ 分片 {part} 完成: 总{total_count}, 删除{deleted_validated}, 过滤{len(rules_to_validate)-len(valid)}")
    print(f"COMMIT_STATS: 总 {total_count}, 删除 {deleted_validated}, 过滤 {len(rules_to_validate)-len(valid)}")

# ===============================
# 主入口
# ===============================
if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--part",help="验证指定分片 1~16")
    parser.add_argument("--force-update",action="store_true",help="强制重新下载规则源并切片")
    args=parser.parse_args()

    if args.force_update or not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR,"part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()

    if args.part:
        process_part(args.part)
    else:
        print("提示: 使用 --part 指定要验证的分片（1~16）")
