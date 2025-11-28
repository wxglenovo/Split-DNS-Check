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
DNS_BATCH_SIZE = 540
WRITE_COUNTER_MAX = 6
DNS_THREADS = 80

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 文件确保函数
# ===============================
def ensure_bin_file(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "wb") as f:
            f.write(msgpack.packb({}, use_bin_type=True))

ensure_bin_file(DELETE_COUNTER_FILE)
ensure_bin_file(NOT_WRITTEN_FILE)
if not os.path.exists(RETRY_FILE):
    open(RETRY_FILE, "w", encoding="utf-8").close()

# ===============================
# msgpack 读写
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
    try:
        with open(path, "wb") as f:
            f.write(msgpack.packb(data, use_bin_type=True))
    except Exception as e:
        print(f"⚠ 保存 {path} 错误: {e}")

# ===============================
# DNS 验证
# ===============================
resolver = dns.resolver.Resolver(configure=True)
resolver.timeout = DNS_TIMEOUT
resolver.lifetime = DNS_TIMEOUT

def check_domain(rule):
    domain = rule.lstrip("|").split("^")[0].replace("*","")
    if not domain:
        return None
    try:
        resolver.resolve(domain)
        return rule
    except:
        return None

def dns_validate(rules, part):
    valid_rules = []
    total_rules = len(rules)
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=DNS_THREADS) as executor:
        futures = {executor.submit(check_domain, r): r for r in rules}
        completed = 0
        for future in as_completed(futures):
            res = future.result() if future.exception() is None else None
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

    rules_to_validate = set()
    updated_delete_counter = {}
    reset_rules = set()
    removed_rules = set()
    skipped_rules = []

    # 处理旧规则
    for rule, cnt in delete_counter.items():
        cnt = int(cnt)
        if cnt >= 118 and rule not in all_rules_set:
            removed_rules.add(rule)
            continue
        if cnt >= 114 and rule in all_rules_set:
            cnt = 80
            reset_rules.add(rule)
        if cnt >= 97:
            skipped_rules.append(rule)
        else:
            rules_to_validate.add(rule)
        updated_delete_counter[rule] = cnt

    # 处理新规则
    for rule in all_rules:
        if rule not in updated_delete_counter:
            updated_delete_counter[rule] = 64
            rules_to_validate.add(rule)

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
    return True
  
# ===============================
# 分片切分
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

    max_iterations = 10
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
# 更新 not_written_counter
# ===============================
def update_not_written_counter(part_num, valid_rules, all_rules_set):
    part_key = f"validated_part_{part_num}"
    counter = load_bin(NOT_WRITTEN_FILE)
    counter.setdefault(part_key, {})

    validated_file = os.path.join(DIST_DIR, f"{part_key}.txt")
    existing_rules = set(open(validated_file, "r", encoding="utf-8").read().splitlines()) if os.path.exists(validated_file) else set()
    part_counter = counter.get(part_key, {})

    valid_rules_set = set(valid_rules)

    # 1️⃣ DNS 成功规则 → write_counter 重置
    for r in valid_rules_set:
        part_counter[r] = WRITE_COUNTER_MAX

    # 2️⃣ 当前分片已有规则但不在 DNS 成功 → write_counter -1
    for r in existing_rules - valid_rules_set:
        part_counter[r] = max(part_counter.get(r, WRITE_COUNTER_MAX) - 1, 0)

    # 3️⃣ write_counter <=1 且不在 all_rules → 删除
    to_remove = [r for r in existing_rules if part_counter.get(r, 0) <= 1 and r not in all_rules_set]
    for r in to_remove:
        existing_rules.discard(r)
        part_counter.pop(r, None)
        print(f"❌ 删除规则 {r}（write_counter<=1 且不在 all_rules）")

    # 4️⃣ write_counter <=0 → 写入 retry_rules
    to_retry = [r for r in existing_rules.union(valid_rules_set) if part_counter.get(r, 0) <= 0]
    if to_retry:
        existing_retry = set()
        if os.path.exists(RETRY_FILE):
            existing_retry.update(l.strip() for l in open(RETRY_FILE, "r", encoding="utf-8") if l.strip())
        new_retry = set(to_retry) - existing_retry
        if new_retry:
            with open(RETRY_FILE, "a", encoding="utf-8") as f:
                f.write("\n".join(sorted(new_retry)) + "\n")
        for r in to_retry:
            existing_rules.discard(r)
            valid_rules_set.discard(r)
            part_counter.pop(r, None)
        print(f"🔥 {len(to_retry)} 条 write_counter<=0 的规则写入 retry_rules.txt（新增 {len(new_retry)} 条）")

    final_rules = sorted(existing_rules.union(valid_rules_set))
    with open(validated_file, "w", encoding="utf-8") as f:
        f.write("\n".join(final_rules))

    counter[part_key] = part_counter
    save_bin(NOT_WRITTEN_FILE, counter)
    return len(to_retry)

# ===============================
# 处理分片
# ===============================
def process_part(part, all_rules_set=None):
    part = int(part)
    part_file = os.path.join(TMP_DIR, f"part_{part:02d}.txt")

    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新拉取规则…")
        all_rules = download_all_sources()
        all_rules_set = set(all_rules)
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8") if l.strip()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    # 插入 retry_rules 顶部
    rules_to_validate = list(lines)
    if os.path.exists(RETRY_FILE):
        with open(RETRY_FILE, "r", encoding="utf-8") as rf:
            retry_rules = [r.strip() for r in rf if r.strip()]
        if retry_rules:
            for r in reversed(retry_rules):
                if r not in rules_to_validate:
                    rules_to_validate.insert(0, r)
            open(RETRY_FILE, "w", encoding="utf-8").truncate(0)
            print(f"🔁 将 {len(retry_rules)} 条 retry_rules 插入分片顶部")

    valid_rules = dns_validate(rules_to_validate, part)
    added_count = len(valid_rules)

    if all_rules_set is None:
        all_rules_set = set(rules_to_validate)

    removed_count = update_not_written_counter(part, valid_rules, all_rules_set)
    print(f"✅ 分片 {part} 更新完成: DNS验证成功 {added_count}, 移除 {removed_count}, 分片总 {len(lines)}")
    print("--------------------------------------------------")

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
