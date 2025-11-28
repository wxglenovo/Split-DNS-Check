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
# 配置区
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
WRITE_COUNTER_MAX = 6
DNS_BATCH_SIZE = 540
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
# Msgpack 读写
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
# 下载并合并规则源
# ===============================
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
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
            print(f"🔄 下载 {url} 成功，获取 {len(new_rules)} 条规则")
        except requests.RequestException as e:
            print(f"⚠ 下载失败 {url}: {e}")

    all_rules_set = set(all_rules)
    print(f"✅ 合并 {len(all_rules)} 条规则")

    # 更新 delete_counter
    delete_counter = load_bin(DELETE_COUNTER_FILE)
    updated_counter = {}
    removed_rules = set()
    skipped_rules = set()
    reset_rules = set()
    rules_to_validate = set()

    for rule, cnt in delete_counter.items():
        cnt = int(cnt) + 1
        if cnt >= 118 and rule not in all_rules_set:
            removed_rules.add(rule)
            continue
        if cnt >= 114 and rule in all_rules_set:
            cnt = 80
            reset_rules.add(rule)
        if cnt >= 97:
            skipped_rules.add(rule)
        else:
            rules_to_validate.add(rule)
        updated_counter[rule] = cnt

    # 新规则
    for rule in all_rules_set:
        if rule not in updated_counter:
            updated_counter[rule] = 64
            rules_to_validate.add(rule)

    save_bin(DELETE_COUNTER_FILE, updated_counter)

    # 输出信息
    if reset_rules:
        print(f"🔢 共 {len(reset_rules)} 条规则 delete_counter≥114，已重置为 80")
    if removed_rules:
        print(f"🗑️ 共 {len(removed_rules)} 条规则 delete_counter≥118 且不在源文件，已移除")
    if skipped_rules:
        print(f"⏩ 共 {len(skipped_rules)} 条规则 delete_counter≥97 被跳过验证")

    print(f"📚 合并总规则 {len(all_rules)} 条，⏩ 跳过 {len(skipped_rules)} 条，🧮 需要验证 {len(rules_to_validate)} 条，🪓 即将切分为 {PARTS} 片")

    # 切分进入验证的规则
    split_parts(list(rules_to_validate), updated_counter)
    return all_rules

# ===============================
# 分片切分
# ===============================
def split_parts(all_rules, delete_counter):

    # -------------------------------------
    # 1) 加载 validated_part_X，并建立 rule→part 映射
    # -------------------------------------
    validated = []
    rule2part = {}

    for i in range(1, PARTS + 1):
        f = os.path.join(DIST_DIR, f"validated_part_%d.txt" % i)
        if os.path.isfile(f):
            with open(f, "r", encoding="utf-8") as ff:
                lines = ff.read().splitlines()
        else:
            lines = []

        s = set(lines)
        validated.append(s)

        # 映射加速：O(N)
        for r in s:
            rule2part[r] = i - 1  # 用 0-based 索引更快

    # -------------------------------------
    # 2) 按 delete_counter 过滤 & 分类 固定 / 可移动
    # -------------------------------------
    dc_get = delete_counter.get

    part_fixed = [[] for _ in range(PARTS)]
    movable_rules = []
    movable_dc = []

    for r in all_rules:
        dc = int(dc_get(r, 64))

        # 删除规则 ≥97
        if dc >= 97:
            continue

        p = rule2part.get(r)

        if p is not None:
            if dc < 20:
                part_fixed[p].append((r, dc))  # 固定规则
            else:
                movable_rules.append(r)
                movable_dc.append(dc)
        else:
            # 新规则：默认可移动
            movable_rules.append(r)
            movable_dc.append(dc)

    # -------------------------------------
    # 3) 按 dc 降序对可移动规则排序（最大优先移动）
    #    使用 argsort 风格，避免 tuple 排序的多重比较
    # -------------------------------------
    idx = sorted(range(len(movable_dc)), key=lambda i: movable_dc[i], reverse=True)
    movable_rules = [movable_rules[i] for i in idx]
    movable_dc = [movable_dc[i] for i in idx]

    # -------------------------------------
    # 4) 初始化分片（仅固定规则）
    # -------------------------------------
    part_buckets = [list(lst) for lst in part_fixed]

    # -------------------------------------
    # 5) 最小堆负载均衡分配可移动规则
    # -------------------------------------
    import heapq

    # 只记录 (size, part_id)，减少对象开销
    heap = [(len(part_buckets[i]), i) for i in range(PARTS)]
    heapq.heapify(heap)

    for r, dc in zip(movable_rules, movable_dc):
        size, idx = heapq.heappop(heap)
        part_buckets[idx].append((r, dc))
        heapq.heappush(heap, (size + 1, idx))

    # -------------------------------------
    # 6) 写文件（固定与可移动均按 dc 排序）
    # -------------------------------------
    os.makedirs(TMP_DIR, exist_ok=True)

    for i in range(PARTS):
        fixed = part_fixed[i]
        movable = [x for x in part_buckets[i] if x not in fixed]  # 不会 O(n²)，因为 part_fixed 很小

        # 只按 dc 排序（最快）
        fixed.sort(key=lambda x: x[1])
        movable.sort(key=lambda x: x[1])

        final_rules = [r for r, dc in (fixed + movable)]

        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(final_rules))

        print(f"📄 分片 {i+1}: {len(final_rules)} 条规则 "
              f"(固定 {len(fixed)} + 移动 {len(movable)})")

# ===============================
# 更新 not_written_counter
# ===============================
def update_not_written_counter(part, valid_rules, all_rules_set):
    part_key = f"validated_part_{part}"
    not_written = load_bin(NOT_WRITTEN_FILE)

    part_counter = not_written.get(part_key, {})

    to_remove_no_retry = []
    to_retry = []

    # 1) DNS 成功规则 -> 重置 counter
    for r in valid_rules:
        part_counter[r] = WRITE_COUNTER_MAX

    # 2) 对老规则 write_counter -=1
    for r in list(part_counter.keys()):
        if r not in valid_rules:
            part_counter[r] -= 1
            if part_counter[r] <= 0:
                to_retry.append(r)
            elif part_counter[r] <= 1 and r not in all_rules_set:
                to_remove_no_retry.append(r)

    # 3) 写入 retry_rules.txt
    old_retry = set()
    if os.path.exists(RETRY_FILE):
        with open(RETRY_FILE, "r", encoding="utf-8") as rf:
            old_retry = set(r.strip() for r in rf if r.strip())

    new_retry = [r for r in to_retry if r not in old_retry]
    if new_retry:
        with open(RETRY_FILE, "a", encoding="utf-8") as rf:
            for r in new_retry:
                rf.write(r + "\n")

    # 4) 删除规则
    for r in to_remove_no_retry + to_retry:
        part_counter.pop(r, None)

    not_written[part_key] = part_counter
    save_bin(NOT_WRITTEN_FILE, not_written)

    removed_count = len(to_remove_no_retry) + len(to_retry)
    removed_no_retry_count = len(to_remove_no_retry)

    return removed_count, new_retry, removed_no_retry_count

# ===============================
# 处理分片
# ===============================
def process_part(part, all_rules_set=None):
    part = int(part)
    part_key = f"validated_part_{part}"
    part_file = os.path.join(TMP_DIR, f"part_{part:02d}.txt")
    validated_file = os.path.join(DIST_DIR, f"{part_key}.txt")

    # 分片不存在时自动下载规则源
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新拉取规则…")
        all_rules = download_all_sources()
        all_rules_set = set(all_rules)
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    # 读取 TMP_DIR 分片规则
    with open(part_file, "r", encoding="utf-8") as f:
        rules_to_validate = [l.strip() for l in f if l.strip()]
    print(f"⏱ 验证分片 {part}, 共 {len(rules_to_validate)} 条规则")

    # 插入 retry_rules
    to_retry_inserted = 0
    if os.path.exists(RETRY_FILE):
        with open(RETRY_FILE, "r", encoding="utf-8") as rf:
            retry_rules = [r.strip() for r in rf if r.strip()]
        for r in reversed(retry_rules):
            if r not in rules_to_validate:
                rules_to_validate.insert(0, r)
                to_retry_inserted += 1
        open(RETRY_FILE, "w", encoding="utf-8").truncate(0)
        if to_retry_inserted:
            print(f"🔁 将 {to_retry_inserted} 条 retry_rules 插入分片顶部")

    # DNS 验证
    valid_rules = set(dns_validate(rules_to_validate, part))
    added_count = len(valid_rules)

    if all_rules_set is None:
        all_rules_set = set(rules_to_validate)

    # 更新 delete_counter
    delete_counter = load_bin(DELETE_COUNTER_FILE)
    for r in valid_rules:
        delete_counter[r] = 0
    for r in rules_to_validate:
        if r not in valid_rules:
            delete_counter[r] = int(delete_counter.get(r, 64)) + 1
    save_bin(DELETE_COUNTER_FILE, delete_counter)

    # ============================================================
    # 读取 DIST_DIR/validated_part_X.txt 已存在的老规则
    # ============================================================
    if os.path.exists(validated_file):
        with open(validated_file, "r", encoding="utf-8") as vf:
            existing_rules = set(line.strip() for line in vf if line.strip())
    else:
        existing_rules = set()

    # 读取 not_written_counter
    counter = load_bin(NOT_WRITTEN_FILE)
    part_counter = counter.get(part_key, {})

    # 使旧规则至少有 write_counter
    for r in existing_rules:
        if r not in part_counter:
            part_counter[r] = WRITE_COUNTER_MAX

    # 调用核心更新逻辑
    removed_count, new_retry, removed_no_retry = update_not_written_counter(
        part, valid_rules, all_rules_set
    )

    # 重新取更新后的 part_counter
    counter_data = load_bin(NOT_WRITTEN_FILE).get(part_key, {})
    final_rules = sorted(counter_data.keys())


    # =============================
    # 写回 DIST_DIR/validated_part_XX.txt  ←（修复关键点）
    # =============================
    with open(validated_file, "w", encoding="utf-8") as vf:
        vf.write("\n".join(final_rules))
    print(f"💾 validated_part_{part}.txt 已更新到: {validated_file}")

    # write_counter 统计
    counts = {i: 0 for i in range(1, WRITE_COUNTER_MAX + 1)}
    for v in counter_data.values():
        if 1 <= v <= WRITE_COUNTER_MAX:
            counts[v] += 1

    # delete_counter 统计
    delete_counts = {}
    for r in final_rules:
        cnt = int(delete_counter.get(r, 0))
        delete_counts[cnt] = delete_counts.get(cnt, 0) + 1

    print("\n📊 当前分片 write_counter 规则统计:")
    for i in range(1, WRITE_COUNTER_MAX + 1):
        if counts[i]:
            print(f"    ⚠ write_counter {i}/{WRITE_COUNTER_MAX} 的规则条数: {counts[i]}")

    print("\n📊 当前分片 delete_counter 规则统计:")
    for k in sorted(delete_counts):
        print(f"    ⚠ delete_counter={k} 的规则条数: {delete_counts[k]}")

    print("--------------------------------------------------")
    print(f"📉 本次 ❌ 删除（write_counter<=1 且不在 all_rules）的规则共有 {removed_no_retry} 条")
    if new_retry:
        print(f"🔥 本次写入 retry_rules.txt 的规则共有 {len(new_retry)} 条")
    print(f"✅ 分片 {part} 更新完成: 总 {len(final_rules)}, DNS 成功 {added_count}, 删除 {removed_count}")
    print(f"COMMIT_STATS: 总 {len(final_rules)}, 新增 {added_count}, 删除 {removed_count}, 过滤 {len(rules_to_validate) - added_count}")


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
