#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, msgpack, requests, argparse, dns.resolver, time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===============================
# 配置区
# ===============================
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
URLS_TXT = "urls.txt"
PARTS = 16
DNS_THREADS = 80
DNS_TIMEOUT = 2
DNS_BATCH_SIZE = 540
WRITE_COUNTER_MAX = 6

HASH_LIST_FILE = os.path.join(DIST_DIR, "hash_list.bin")
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.bin")
NOT_WRITTEN_FILE = os.path.join(DIST_DIR, "not_written_counter.bin")
RETRY_FILE = os.path.join(DIST_DIR, "retry_rules.txt")

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 文件读写
# ===============================
def load_bin(path):
    try:
        with open(path,"rb") as f:
            raw=f.read()
            return msgpack.unpackb(raw, raw=False) if raw else {}
    except Exception as e:
        print(f"⚠ 读取 {path} 错误: {e}")
        return {}

def save_bin(path, data):
    try:
        with open(path,"wb") as f:
            f.write(msgpack.packb(data,use_bin_type=True))
    except Exception as e:
        print(f"⚠ 保存 {path} 错误: {e}")

# ===============================
# 下载 & 分片
# ===============================
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在"); return []
    all_rules=[]
    with open(URLS_TXT,"r",encoding="utf-8") as f: urls=[u.strip() for u in f if u.strip()]
    for url in urls:
        try:
            r=requests.get(url,timeout=20); r.raise_for_status()
            all_rules.extend([l.strip() for l in r.text.splitlines() if l.strip()])
        except: print(f"⚠ 下载失败 {url}")
    all_rules=sorted(set(all_rules))
    with open(MASTER_RULE,"w",encoding="utf-8") as f: f.write("\n".join(all_rules))
    # 分片
    part_size=(len(all_rules)+PARTS-1)//PARTS
    for i in range(PARTS):
        with open(os.path.join(TMP_DIR,f"part_{i+1:02d}.txt"),"w",encoding="utf-8") as f:
            f.write("\n".join(all_rules[i*part_size:(i+1)*part_size]))
    print(f"✅ 下载完成，规则总数: {len(all_rules)}")
    return all_rules

# ===============================
# DNS 验证
# ===============================
def check_domain(rule):
    resolver=dns.resolver.Resolver(); resolver.timeout=DNS_TIMEOUT; resolver.lifetime=DNS_TIMEOUT
    domain=rule.lstrip("|").split("^")[0].replace("*","")
    if not domain: return None
    try: resolver.resolve(domain); return rule
    except: return None

def dns_validate(rules,part):
    valid=[]
    total=len(rules); start=time.time()
    with ThreadPoolExecutor(max_workers=DNS_THREADS) as executor:
        futures={executor.submit(check_domain,r):r for r in rules}
        completed=0
        for f in as_completed(futures):
            res=f.result() if f else None
            if res: valid.append(res)
            completed+=1
            if completed%DNS_BATCH_SIZE==0 or completed==total:
                elapsed=time.time()-start
                speed=completed/elapsed if elapsed else 0
                eta=(total-completed)/speed if speed else 0
                print(f"✅ 已验证 {completed}/{total} | 有效 {len(valid)} | 速度 {speed:.1f}/秒 | 预计 {eta:.1f}s")
    return valid

# ===============================
# 更新 write_counter
# ===============================
def update_not_written_counter(part_num, valid_rules, all_rules):
    part_key=f"validated_part_{part_num}"
    counter=load_bin(NOT_WRITTEN_FILE)
    counter.setdefault(part_key,{})
    validated_file=os.path.join(DIST_DIR,f"{part_key}.txt")
    existing=set(open(validated_file,"r",encoding="utf-8").read().splitlines()) if os.path.exists(validated_file) else set()
    part_counter=counter.get(part_key,{})
    for r in valid_rules: part_counter[r]=WRITE_COUNTER_MAX
    valid_set=set(valid_rules)
    for r in existing-valid_set: part_counter[r]=max(part_counter.get(r,WRITE_COUNTER_MAX)-1,0)
    to_remove=[r for r in existing if part_counter.get(r,0)<=0]
    if to_remove:
        with open(RETRY_FILE,"a",encoding="utf-8") as rf: rf.write("\n".join(to_remove)+"\n")
        print(f"🔥 {len(to_remove)} 条 write_counter<=0 写入 retry_rules.txt")
        for r in to_remove: existing.discard(r); part_counter.pop(r,None)
    final=sorted(existing.union(valid_set))
    with open(validated_file,"w",encoding="utf-8") as f: f.write("\n".join(final))
    counter[part_key]=part_counter
    save_bin(NOT_WRITTEN_FILE,counter)
    return len(to_remove)

# ===============================
# 分片处理
# ===============================
def process_part(part, all_rules, delete_counter):
    part_file=os.path.join(TMP_DIR,f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file): all_rules=download_all_sources()
    lines=[l.strip() for l in open(part_file,"r",encoding="utf-8").read().splitlines() if l.strip()]
    print(f"⏱ 分片 {part} 共 {len(lines)} 条规则")
    rules=[r for r in lines if int(delete_counter.get(r,0))<WRITE_COUNTER_MAX]
    retry=set(open(RETRY_FILE,"r",encoding="utf-8").read().splitlines()) if os.path.exists(RETRY_FILE) else set()
    rules=list(retry)+[r for r in rules if r not in retry]
    open(RETRY_FILE,"w",encoding="utf-8").truncate(0)
    valid=set(dns_validate(rules,part))
    for r in rules: delete_counter[r]=0 if r in valid else delete_counter.get(r,0)+1
    save_bin(DELETE_COUNTER_FILE,delete_counter)
    deleted=update_not_written_counter(part,list(valid),all_rules)
    print(f"✅ 分片 {part} 完成: 有效 {len(valid)}, write_counter<=0 删除 {deleted}")

# ===============================
# 主入口
# ===============================
if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--part",help="指定分片 1~16")
    parser.add_argument("--force-update",action="store_true",help="强制下载")
    args=parser.parse_args()

    delete_counter=load_bin(DELETE_COUNTER_FILE)
    if args.force_update or not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR,"part_01.txt")):
        all_rules=download_all_sources()
    else:
        with open(MASTER_RULE,"r",encoding="utf-8") as f: all_rules=[l.strip() for l in f if l.strip()]
    if args.part: process_part(args.part,all_rules,delete_counter)
    else: print("提示: 使用 --part 指定要验证的分片（1~16）")
