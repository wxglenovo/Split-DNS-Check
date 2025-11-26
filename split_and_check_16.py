#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
split_and_check_16.py
修复版：稳定处理 hash_list.bin（保存为 dict(rule -> sha256_hex)）、避免 msgpack 超大整数问题、
保留原有 delete_counter / not_written_counter 逻辑。
"""

import os
import sys
import msgpack
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import hashlib
import pickle
import traceback

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
# 辅助：安全初始化 msgpack 文件
# ===============================
def ensure_msgpack_dict(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        try:
            with open(path, "wb") as f:
                f.write(msgpack.packb({}, use_bin_type=True))
            print(f"✅ 已初始化 {path}")
        except Exception as e:
            print(f"⚠ 初始化 {path} 失败: {e}")

ensure_msgpack_dict(DELETE_COUNTER_FILE)
ensure_msgpack_dict(NOT_WRITTEN_FILE)
if not os.path.exists(RETRY_FILE):
    open(RETRY_FILE, "w", encoding="utf-8").close()
    print("✅ 已创建 retry_rules.txt")

# 确保 hash_list.bin 至少存在（我们用 pickle 存储 dict）
if not os.path.exists(HASH_LIST_FILE):
    try:
        with open(HASH_LIST_FILE, "wb") as f:
            pickle.dump({}, f)
        print("✅ 已创建空的 hash_list.bin")
    except Exception as e:
        print(f"⚠ 创建 hash_list.bin 失败: {e}")

# ===============================
# msgpack 读取/写入（用于 delete_counter / not_written_counter）
# ===============================
def load_msgpack(path):
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

def save_msgpack(path, data):
    try:
        with open(path, "wb") as f:
            f.write(msgpack.packb(data, use_bin_type=True))
    except Exception as e:
        print(f"⚠ 保存 {path} 错误: {e}")

# ===============================
# hash_list (pickle) 读取/写入（并兼容旧格式）
# 存储结构：dict { rule_string: sha256_hex_string }
# ===============================
def load_hash_list(filename):
    if not os.path.exists(filename):
        return {}
    try:
        with open(filename, "rb") as f:
            data = pickle.load(f)
    except Exception as e:
        print(f"⚠ 读取 {filename} 出错（pickle），尝试重建空表: {e}")
        return {}

    # 兼容多种旧格式
    if isinstance(data, dict):
        # 可能 value 是大整数（旧版），或 hex 字符串（我们希望）
        newd = {}
        for k, v in data.items():
            # key 有可能是规则，也可能是 hash——若 key 看起来像 hex 或 int 则跳过
            if isinstance(k, bytes):
                try:
                    k = k.decode("utf-8")
                except:
                    k = str(k)
            if isinstance(v, int):
                # 转为 hex 字符串
                try:
                    h = format(v, 'x')
                    # 保证长度为 64（sha256 hex）
                    if len(h) % 2 == 1:
                        h = "0" + h
                    h = h.lower()
                    newd[k] = h
                except Exception:
                    newd[k] = str(v)
            else:
                newd[k] = str(v)
        return newd
    elif isinstance(data, list) or isinstance(data, set):
        # 旧版可能直接存储规则列表或 hash list -> 将其转换为 dict(rule -> sha256_hex)
        newd = {}
        for rule in data:
            try:
                rule_s = rule.decode('utf-8') if isinstance(rule, bytes) else str(rule)
                h = hashlib.sha256(rule_s.encode('utf-8')).hexdigest()
                newd[rule_s] = h
            except Exception:
                continue
        return newd
    else:
        return {}

def save_hash_list(hashes, filename):
    """
    hashes: dict { rule: sha256_hex }
    """
    try:
        with open(filename, "wb") as f:
            pickle.dump(hashes, f, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        print(f"⚠ 保存 {filename} 失败: {e}")

# ===============================
# DNS helper
# ===============================
def check_domain(rule):
    resolver = dns.resolver.Resolver()
    resolver.timeout = DNS_TIMEOUT
    resolver.lifetime = DNS_TIMEOUT
    # 提取域名：类似你原先的处理
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
        print("❌ urls.txt 不存在，无法下载规则")
        return []

    print("📥 下载规则源...")
    all_rules = []
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]

    for url in urls:
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            new_rules = [line.strip() for line in r.text.splitlines() if line.strip()]
            all_rules.extend(new_rules)
            print(f"🔄 下载 {url} 成功：{len(new_rules)} 条")
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")

    # 去重、排序并写入 MASTER_RULE
    all_rules = sorted(set(all_rules))
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(all_rules))
    print(f"✅ 合并写入 {MASTER_RULE}，共 {len(all_rules)} 条")

    # 读取并修复 hash_list（将所有规则确保在 hash_list 中）
    hash_list = load_hash_list(HASH_LIST_FILE)
    if not isinstance(hash_list, dict):
        hash_list = {}
    added = 0
    for rule in all_rules:
        if rule not in hash_list:
            hash_list[rule] = hashlib.sha256(rule.encode('utf-8')).hexdigest()
            added += 1
    if added:
        save_hash_list(hash_list, HASH_LIST_FILE)
        print(f"✅ hash_list.bin 更新：新增 {added} 条")
    else:
        print("ℹ️ hash_list.bin 无需更新（已包含所有规则）")

    # 切片
    split_parts(all_rules, load_msgpack(DELETE_COUNTER_FILE))
    return all_rules

# ===============================
# 切分与负载（使用 hash_list 中的 hex 值排序）
# ===============================
def split_parts(all_rules, delete_counter):
    # all_rules: list
    hash_list = load_hash_list(HASH_LIST_FILE)
    # 保证返回 dict
    if not isinstance(hash_list, dict):
        hash_list = {}

    # 为每条规则准备排序键（转换 hex->int）
    def key_for(rule):
        h = hash_list.get(rule)
        if not h:
            # 生成并保存
            h = hashlib.sha256(rule.encode('utf-8')).hexdigest()
            hash_list[rule] = h
        try:
            return int(h, 16)
        except Exception:
            # fallback
            return int(hashlib.sha256(rule.encode('utf-8')).hexdigest()[:16], 16)

    sorted_rules = sorted(all_rules, key=lambda r: key_for(r))

    # 平均切分
    part_buckets = [[] for _ in range(PARTS)]
    for idx, rule in enumerate(sorted_rules):
        part_buckets[idx % PARTS].append(rule)

    # 写入分片文件
    os.makedirs(TMP_DIR, exist_ok=True)
    for i, bucket in enumerate(part_buckets):
        path = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(bucket))
        print(f"📄 写入分片 {i+1}: {len(bucket)} 条 -> {path}")

    # 保存 hash_list（如果新增了）
    try:
        save_hash_list(hash_list, HASH_LIST_FILE)
    except Exception as e:
        print(f"⚠ 保存 hash_list 失败: {e}")

# ===============================
# DNS 验证（并行）
# 返回有效规则列表
# ===============================
def dns_validate(rules, part):
    valid = []
    total = len(rules)
    start = time.time()
    if total == 0:
        return valid

    with ThreadPoolExecutor(max_workers=DNS_THREADS) as executor:
        futures = {executor.submit(check_domain, r): r for r in rules}
        done = 0
        for fut in as_completed(futures):
            try:
                res = fut.result()
            except Exception:
                res = None
            if res:
                valid.append(res)
            done += 1
            if done % DNS_BATCH_SIZE == 0 or done == total:
                elapsed = time.time() - start
                speed = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / speed if speed > 0 else 0
                print(f"✅ 已验证 {done}/{total} 条 | 有效 {len(valid)} 条 | 速度 {speed:.1f}/秒 | 预计完成 {eta:.1f}s")
    return valid

# ===============================
# 更新 not_written_counter（核心逻辑）
# ===============================
def update_not_written_counter(part_num, valid_rules, all_rules):
    part_key = f"validated_part_{part_num}"
    counter = load_msgpack(NOT_WRITTEN_FILE) or {}
    # ensure partitions exist
    for i in range(1, PARTS + 1):
        counter.setdefault(f"validated_part_{i}", {})

    validated_file = os.path.join(DIST_DIR, f"{part_key}.txt")
    existing_rules = set(open(validated_file, "r", encoding="utf-8").read().splitlines()) if os.path.exists(validated_file) else set()
    all_rules_set = set(all_rules or [])
    part_counter = counter.get(part_key, {})

    # 新验证成功规则 -> 重置计数
    for r in valid_rules:
        part_counter[r] = WRITE_COUNTER_MAX

    valid_rules_set = set(valid_rules)

    # 旧规则但本次没出现 -> 递减
    for r in existing_rules - valid_rules_set:
        part_counter[r] = max(part_counter.get(r, WRITE_COUNTER_MAX) - 1, 0)

    # write_counter == 1 且不在 all_rules -> 删除
    to_remove = [r for r in list(existing_rules) if part_counter.get(r, 0) == 1 and r not in all_rules_set]
    for r in to_remove:
        print(f"❌ 删除规则 {r}（write_counter=1 且不在 all_rules）")
        existing_rules.discard(r)
        part_counter.pop(r, None)

    # write_counter <= 0 -> 写入 retry_rules.txt 并删除
    to_retry = [r for r in existing_rules if part_counter.get(r, 0) <= 0]
    if to_retry:
        existing_retry = set(open(RETRY_FILE, "r", encoding="utf-8").read().splitlines()) if os.path.exists(RETRY_FILE) else set()
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
    save_msgpack(NOT_WRITTEN_FILE, counter)
    return len(to_retry)

# ===============================
# 处理单个分片
# ===============================
def process_part(part):
    part = int(part)
    part_file = os.path.join(TMP_DIR, f"part_{part:02d}.txt")

    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，尝试下载并切分规则")
        download_all_sources()

    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").read().splitlines() if l.strip()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    old_rules = set(open(out_file, "r", encoding="utf-8").read().splitlines()) if os.path.exists(out_file) else set()

    delete_counter = load_msgpack(DELETE_COUNTER_FILE) or {}
    rules_to_validate = [r for r in lines if int(delete_counter.get(r, 4)) < 7]

    # 对 delete_counter >= 7 的规则，+1（保持计数增长）
    for r in lines:
        if int(delete_counter.get(r, 4)) >= 7:
            delete_counter[r] = int(delete_counter.get(r, 4)) + 1

    # 读取 retry_rules 并加入验证队列顶部
    retry_rules = []
    if os.path.exists(RETRY_FILE):
        with open(RETRY_FILE, "r", encoding="utf-8") as rf:
            retry_rules = [r.strip() for r in rf if r.strip()]
        if retry_rules:
            print(f"🔁 将 {len(retry_rules)} 条 retry_rules 插入分片顶部")
            for r in reversed(retry_rules):
                if int(delete_counter.get(r, 4)) < 7 and r not in rules_to_validate:
                    rules_to_validate.insert(0, r)
            # 清空 retry 文件（避免下一分片重复使用相同重试项）
            open(RETRY_FILE, "w", encoding="utf-8").truncate(0)

    # DNS 验证
    valid = set(dns_validate(rules_to_validate, part))
    added_count = 0
    failure_counts = {}

    # 更新 delete_counter，同时收集 write_counter<=0 规则到 retry（在 update_not_written_counter 有更完整处理）
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

    save_msgpack(DELETE_COUNTER_FILE, delete_counter)

    # 下载 all_rules（用于 update_not_written_counter）
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

    # 打印统计
    print("\n📊 当前分片连续失败统计:")
    for i in range(1, WRITE_COUNTER_MAX + 1):
        if failure_counts.get(i, 0) > 0:
            print(f"    ⚠ 连续失败 {i}/{WRITE_COUNTER_MAX} 的规则条数: {failure_counts[i]}")

    print("\n📊 当前分片 write_counter 规则统计:")
    part_key = f"validated_part_{part}"
    counter = load_msgpack(NOT_WRITTEN_FILE) or {}
    part_counter = counter.get(part_key, {})
    counts = {i: 0 for i in range(1, WRITE_COUNTER_MAX + 1)}
    for v in part_counter.values():
        try:
            vv = int(v)
        except:
            vv = 0
        if 1 <= vv <= WRITE_COUNTER_MAX:
            counts[vv] += 1
    total_rules = sum(counts.values())
    print(f"    ℹ️ 总规则条数: {total_rules}")
    for i in range(1, WRITE_COUNTER_MAX + 1):
        if counts[i] > 0:
            print(f"    ⚠ write_counter {i}/{WRITE_COUNTER_MAX} 的规则条数: {counts[i]}")
    print("--------------------------------------------------")

    # 保存最终验证结果（仅 DNS 验证通过的规则）
    with open(out_file, "w", encoding="utf-8") as f:
        for r in sorted(valid):
            f.write(r + "\n")

    print(f"✅ 分片 {part} 完成: 总{total_count}, 新增{added_count}, 删除{deleted_validated}, 过滤{len(rules_to_validate) - len(valid)}")
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {deleted_validated}, 过滤 {len(rules_to_validate) - len(valid)}")

# ===============================
# 主入口
# ===============================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则源并切片")
    args = parser.parse_args()

    # 载入 / 修复 hash_list
    try:
        hl = load_hash_list(HASH_LIST_FILE)
        if not isinstance(hl, dict):
            hl = {}
            save_hash_list(hl, HASH_LIST_FILE)
    except Exception as e:
        print(f"⚠ 载入 hash_list 失败，重建空表: {e}")
        hl = {}
        save_hash_list(hl, HASH_LIST_FILE)

    # 载入 delete_counter
    try:
        dc = load_msgpack(DELETE_COUNTER_FILE)
        if not isinstance(dc, dict):
            dc = {}
            save_msgpack(DELETE_COUNTER_FILE, dc)
    except Exception as e:
        print(f"⚠ 载入 delete_counter 失败，重建空表: {e}")
        dc = {}
        save_msgpack(DELETE_COUNTER_FILE, dc)

    # 若需要强制更新或缺少 master/part 文件则下载并分片
    if args.force_update or not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()

    if args.part:
        process_part(int(args.part))
    else:
        print("提示: 使用 --part 指定要验证的分片（1~16）")

if __name__ == "__main__":
    main()
