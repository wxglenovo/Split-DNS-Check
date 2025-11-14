#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import hashlib

# ===============================
# 配置区（Config）
# ===============================
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
DNS_TIMEOUT = 2
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
NOT_WRITTEN_FILE = os.path.join(DIST_DIR, "not_written_counter.json")
RETRY_FILE = os.path.join(DIST_DIR, "retry_rules.txt")
DELETE_THRESHOLD = 4
DNS_BATCH_SIZE = 540  # 每批540条
WRITE_COUNTER_MAX = 6
DNS_THREADS = 80      # 固定80线程

# 哈希分片微调参数
BALANCE_THRESHOLD = 2
BALANCE_MOVE_LIMIT = 50

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 文件确保函数（静默版）
# ===============================
def ensure_file(path, default_content=""):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(default_content)

ensure_file(DELETE_COUNTER_FILE, "{}")
ensure_file(NOT_WRITTEN_FILE, "{}")
ensure_file(RETRY_FILE, "")

# ===============================
# JSON 读写
# ===============================
def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠ 读取 {path} 时发生错误: {e}")
            return {}
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)
        return {}

def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✅ 已保存 {path}")
    except Exception as e:
        print(f"⚠ 保存 {path} 时发生错误: {e}")

# ===============================
# 单条规则 DNS 验证函数
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
    merged = set()

    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]

    for url in urls:
        print(f"🌐 获取 {url}")
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            for line in r.text.splitlines():
                line = line.strip()
                if line:
                    merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")

    print(f"✅ 合并 {len(merged)} 条规则")

    sorted_rules = sorted(merged)
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted_rules))

    filtered_rules, updated_delete_counter = filter_and_update_high_delete_count_rules(merged)
    split_parts(filtered_rules)

    save_json(DELETE_COUNTER_FILE, updated_delete_counter)

    if os.path.exists(RETRY_FILE):
        with open(RETRY_FILE, "r", encoding="utf-8") as rf:
            retry_rules = [r.strip() for r in rf if r.strip()]
        if retry_rules:
            print(f"🔁 检测到 {len(retry_rules)} 条重试规则，将加入合并规则")
            merged.update(retry_rules)
            with open(MASTER_RULE, "a", encoding="utf-8") as f:
                f.write("\n" + "\n".join(sorted(set(retry_rules))))

    return True

# ===============================
# 删除计数 >=7 的规则过滤
# ===============================
def filter_and_update_high_delete_count_rules(all_rules_set):
    delete_counter = load_json(DELETE_COUNTER_FILE)
    low_delete_count_rules = set()
    updated_delete_counter = delete_counter.copy()

    skipped_rules = []
    reset_rules = []

    for rule in all_rules_set:
        del_cnt = delete_counter.get(rule, 4)
        if del_cnt < 7:
            low_delete_count_rules.add(rule)
        else:
            skipped_rules.append(rule)
            updated_delete_counter[rule] = del_cnt + 1
            if updated_delete_counter[rule] >= 24:
                updated_delete_counter[rule] = 6
                reset_rules.append(rule)

    for rule in skipped_rules[:20]:
        print(f"⚠ 删除计数 ≥7，跳过验证：{rule}")
    print(f"🔢 共 {len(skipped_rules)} 条规则被跳过验证（删除计数≥7）")

    for rule in reset_rules[:20]:
        print(f"🔁 删除计数达到24，重置为 6：{rule}")
    print(f"🔢 共 {len(reset_rules)} 条规则的删除计数达到24，已重置为 6")

    return low_delete_count_rules, updated_delete_counter

# ===============================
# 哈希分片 + 轻量微调平衡
# ===============================
def split_parts(merged_rules):
    sorted_rules = sorted(merged_rules)
    total = len(sorted_rules)
    print(f"🪓 分片 {total} 条规则，分为 {PARTS} 片")

    # 哈希分片
    part_buckets = [[] for _ in range(PARTS)]
    for rule in sorted_rules:
        h = int(hashlib.sha256(rule.encode("utf-8")).hexdigest(), 16)
        idx = h % PARTS
        part_buckets[idx].append(rule)

    # 轻量微调
    while True:
        lens = [len(b) for b in part_buckets]
        max_len, min_len = max(lens), min(lens)
        if max_len - min_len <= BALANCE_THRESHOLD:
            break
        max_idx, min_idx = lens.index(max_len), lens.index(min_len)
        move_count = min(BALANCE_MOVE_LIMIT, (max_len - min_len)//2)
        part_buckets[min_idx].extend(part_buckets[max_idx][-move_count:])
        part_buckets[max_idx] = part_buckets[max_idx][:-move_count]

    for i, bucket in enumerate(part_buckets):
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(bucket))
        print(f"📄 分片 {i+1}: {len(bucket)} 条 → {filename}")

# ===============================
# DNS 验证 + retry_rules 插入顶部
# ===============================
def dns_validate(rules, part):
    retry_rules = []
    if os.path.exists(RETRY_FILE):
        with open(RETRY_FILE, "r", encoding="utf-8") as rf:
            retry_rules = [l.strip() for l in rf if l.strip()]

    combined_rules = retry_rules + rules if retry_rules else rules
    tmp_file = os.path.join(TMP_DIR, f"vpart_{part}.tmp")
    with open(tmp_file, "w", encoding="utf-8") as f:
        f.write("\n".join(combined_rules))

    if retry_rules:
        with open(RETRY_FILE, "w", encoding="utf-8") as f:
            f.write("")
        print(f"🔁 将 {len(retry_rules)} 条 retry_rules 插入分片顶部并清空 {RETRY_FILE}")

    valid_rules = []
    total_rules = len(combined_rules)
    with ThreadPoolExecutor(max_workers=DNS_THREADS) as executor:
        futures = {executor.submit(check_domain, r): r for r in combined_rules}
        completed, start_time = 0, time.time()
        for future in as_completed(futures):
            res = future.result()
            if res: valid_rules.append(res)
            completed += 1
            if completed % DNS_BATCH_SIZE == 0 or completed == total_rules:
                elapsed = time.time() - start_time
                speed = completed / elapsed
                eta = (total_rules - completed)/speed if speed > 0 else 0
                print(f"✅ 已验证 {completed}/{total_rules} 条 | 有效 {len(valid_rules)}条 | 速度 {speed:.1f}条/秒 | 预计完成时间 {eta:.1f} 秒")

    return valid_rules

# ===============================
# 更新 not_written_counter.json
# ===============================
def update_not_written_counter(part_num):
    part_key = f"validated_part_{part_num}"
    counter = load_json(NOT_WRITTEN_FILE)
    for i in range(1, PARTS+1):
        counter.setdefault(f"validated_part_{i}", {})

    validated_file = os.path.join(DIST_DIR, f"{part_key}.txt")
    tmp_file = os.path.join(TMP_DIR, f"vpart_{part_num}.tmp")
    existing_rules = set(open(validated_file, "r", encoding="utf-8").read().splitlines()) if os.path.exists(validated_file) else set()
    tmp_rules = set(open(tmp_file, "r", encoding="utf-8").read().splitlines()) if os.path.exists(tmp_file) else set()

    part_counter = counter.get(part_key, {})
    for r in tmp_rules: part_counter[r] = WRITE_COUNTER_MAX
    for r in existing_rules - tmp_rules: part_counter[r] = part_counter.get(r, WRITE_COUNTER_MAX) - 1

    to_retry = [r for r in existing_rules if part_counter.get(r,0) <= 0]
    if to_retry:
        with open(RETRY_FILE, "a", encoding="utf-8") as rf:
            rf.write("\n".join(to_retry) + "\n")
        print(f"🔥 {len(to_retry)} 条 write_counter ≤ 0 的规则写入 {RETRY_FILE}")
        existing_rules -= set(to_retry)

    with open(validated_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(existing_rules.union(tmp_rules))))

    for r in to_retry: part_counter.pop(r,None)
    counter[part_key] = part_counter
    save_json(NOT_WRITTEN_FILE, counter)
    return len(to_retry)

# ===============================
# 处理分片
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

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").read().splitlines()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    old_rules = set(open(out_file, "r", encoding="utf-8").read().splitlines()) if os.path.exists(out_file) else set()

    delete_counter = load_json(DELETE_COUNTER_FILE)
    rules_to_validate = [r for r in lines if delete_counter.get(r,4)<7]
    for r in lines:
        if delete_counter.get(r,4) >= 7: delete_counter[r] += 1

    final_rules = set(old_rules)
    valid = dns_validate(rules_to_validate, part)
    added_count = 0
    for r in rules_to_validate:
        if r in valid:
            final_rules.add(r)
            delete_counter[r] = 0
            added_count += 1
        else:
            delete_counter[r] = delete_counter.get(r,0)+1
            if delete_counter[r]>=DELETE_THRESHOLD: final_rules.discard(r)

    save_json(DELETE_COUNTER_FILE, delete_counter)
    deleted_validated = update_not_written_counter(part)
    total_count = len(final_rules)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    print(f"✅ 分片 {part} 完成: 总{total_count}, 新增{added_count}, 删除{deleted_validated}, 过滤{len(rules_to_validate)-len(valid)}")
    print(f"COMMIT_STATS:总{total_count},新增{added_count},删除{deleted_validated},过滤{len(rules_to_validate)-len(valid)}")

# ===============================
# 主入口
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则源并切片")
    args = parser.parse_args()

    if args.force_update:
        download_all_sources()

    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR,"part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()

    if args.part:
        process_part(args.part)
