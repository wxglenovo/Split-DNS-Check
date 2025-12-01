#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import msgpack
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from collections import Counter
from collections import deque


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
    split_parts(list(rules_to_validate), updated_counter)
    return all_rules


# ===============================
# 分片切分
# ===============================
def split_parts(rules_to_validate, delete_counter):
    import os
    from collections import deque

    # -------------------------
    # 预处理 delete_counter → group_dc
    # -------------------------
    def group_dc(dc):
        dc = int(dc)
        if dc >= 97:
            return 3      # 忽略
        if dc <= 16:
            return 0      # A
        if dc <= 64:
            return 1      # B
        if dc <= 96:
            return 2      # C

    # -------------------------
    # 1. 读取 validated_part_X，确定 A 的原分片
    # -------------------------
    part_A = [[] for _ in range(PARTS)]  # 固定A
    part_orig_map = {}                   # rule → 原分片编号（0-based）

    for i in range(PARTS):
        path = os.path.join(DIST_DIR, f"validated_part_{i+1}.txt")
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                for r in f.read().splitlines():
                    g = group_dc(delete_counter.get(r, 64))
                    if g == 0:
                        part_A[i].append(r)
                        part_orig_map[r] = i

    # -------------------------
    # 2. 分类所有规则：A / B / C
    # -------------------------
    A_rules = set()
    B_rules = []
    C_rules = []

    for r in rules_to_validate:
        g = group_dc(delete_counter.get(r, 64))
        if g == 0:
            A_rules.add(r)
        elif g == 1:
            B_rules.append((int(delete_counter.get(r, 64)), r))
        elif g == 2:
            C_rules.append(r)
        # g==3 忽略 >=97

    # -------------------------
    # 3. 初始化分片桶
    # -------------------------
    buckets = [deque(part_A[i]) for i in range(PARTS)]
    bucket_sizes = [len(buckets[i]) for i in range(PARTS)]
    A_counts = [len(part_A[i]) for i in range(PARTS)]
    A_max = max(A_counts)

    # -------------------------
    # 4. 用 B 补齐 A 不足的分片
    # -------------------------
    B_rules.sort(key=lambda x: x[0])  # delete_counter 小 → 大
    B_index = 0
    B_len = len(B_rules)

    # 让 B 优先分配给 A 规则最少的分片
    for i in range(PARTS):
        need = A_max - A_counts[i]
        while need > 0 and B_index < B_len:
            _, r = B_rules[B_index]
            # 找到 A 最少的分片并将 B 规则添加进去
            min_A_index = A_counts.index(min(A_counts))  
            buckets[min_A_index].append(r)
            bucket_sizes[min_A_index] += 1
            A_counts[min_A_index] += 1
            B_index += 1
            need -= 1

    # 剩余 B
    B_remaining = [r for _, r in B_rules[B_index:]]

    # -------------------------
    # 5. 剩余 B 均衡分配（最少优先）
    # -------------------------
    for r in B_remaining:
        idx = bucket_sizes.index(min(bucket_sizes))
        buckets[idx].append(r)
        bucket_sizes[idx] += 1

    # -------------------------
    # 6. 使用 C 做最终负载均衡
    # -------------------------
    for r in C_rules:
        idx = bucket_sizes.index(min(bucket_sizes))
        buckets[idx].append(r)
        bucket_sizes[idx] += 1

    # -------------------------
    # 7. 微调 ±1
    # -------------------------
    while True:
        maxi = bucket_sizes.index(max(bucket_sizes))
        mini = bucket_sizes.index(min(bucket_sizes))
        if bucket_sizes[maxi] - bucket_sizes[mini] <= 1:
            break
        rule = buckets[maxi].pop()
        buckets[mini].append(rule)
        bucket_sizes[maxi] -= 1
        bucket_sizes[mini] += 1

    # -------------------------
    # 8. 输出 part_X 文件与日志
    # -------------------------
    os.makedirs(TMP_DIR, exist_ok=True)

    for i in range(PARTS):
        rules = list(buckets[i])

        # 统计 group_dc 分布
        gcount = {0:0, 1:0, 2:0, 3:0}  # g0:A, g1:B, g2:C, g3:忽略
        for r in rules:
            dc = int(delete_counter.get(r, 64))
            if dc <= 16:
                g = 0
            elif dc <= 64:
                g = 1
            elif dc <= 96:
                g = 2
            else:
                g = 3
            gcount[g] += 1

        # 数量统计
        fixed_A = gcount[0]
        move_B = gcount[1]
        move_C = gcount[2]

        # 写文件（UTF-8 BOM）
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8-sig", newline="\n") as f:
            for r in rules:
                f.write(r + "\n")

        # group_dc 输出
        gtext = ", ".join([f"g{k}:{v}" for k,v in sorted(gcount.items()) if v>0])

        # 日志输出
        print(
            f"📄 分片 {i+1}: {len(rules)} 条规则 "
            f"(固定A {fixed_A} + 移动B {move_B} + 移动C {move_C}) | "
            f"group_dc 分布: {gtext}"
        )


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

    # ⭐ 保存原始规则数量（修复 NameError）
    total_rules = len(rules_to_validate)

    # 插入 retry_rules（优化：使用 set，加速判断）
    to_retry_inserted = 0
    if os.path.exists(RETRY_FILE):
        with open(RETRY_FILE, "r", encoding="utf-8") as rf:
            retry_rules = [r.strip() for r in rf if r.strip()]

        if retry_rules:
            current_set = set(rules_to_validate)
            insert_rules = [r for r in retry_rules if r not in current_set]
            if insert_rules:
                rules_to_validate = insert_rules + rules_to_validate
                to_retry_inserted = len(insert_rules)
            # 清空 retry 文件（已将待重试规则插入）
            open(RETRY_FILE, "w", encoding="utf-8").close()

            if to_retry_inserted:
                print(
                    f"🔁 将 {to_retry_inserted} 条 retry_rules 插入分片 {part} 顶部 {total_rules} 条  共计 {len(rules_to_validate)} 条 "                 
                )

    # DNS 验证
    valid_rules = set(dns_validate(rules_to_validate, part))
    added_count = len(valid_rules)

    if all_rules_set is None:
        # all_rules_set 如果为空，就以当前合并后的 rules_to_validate 为准（含 retry 插入）
        all_rules_set = set(rules_to_validate)

    # 更新 delete_counter
    delete_counter = load_bin(DELETE_COUNTER_FILE)
    for r in rules_to_validate:
        delete_counter[r] = 0 if r in valid_rules else int(delete_counter.get(r, 64)) + 1
    save_bin(DELETE_COUNTER_FILE, delete_counter)

    # 读取 DIST_DIR/validated_part_X.txt 已存在的老规则
    if os.path.exists(validated_file):
        with open(validated_file, "r", encoding="utf-8") as vf:
            existing_rules = set(line.strip() for line in vf if line.strip())
    else:
        existing_rules = set()

    # 读取 not_written_counter，并确保旧规则至少有 write_counter，然后写回
    counter = load_bin(NOT_WRITTEN_FILE)
    part_counter = counter.get(part_key, {})
    for r in existing_rules:
        if r not in part_counter:
            part_counter[r] = WRITE_COUNTER_MAX
    # 把更新后的 part_counter 写回文件，确保后续 update_not_written_counter 能读取到这些值
    counter[part_key] = part_counter
    save_bin(NOT_WRITTEN_FILE, counter)

    # 核心更新逻辑
    removed_count, new_retry, removed_no_retry = update_not_written_counter(
        part, valid_rules, all_rules_set
    )

    # 重新读取更新后的 part_counter
    counter_data = load_bin(NOT_WRITTEN_FILE).get(part_key, {})
    final_rules = sorted(counter_data.keys())

    # 写回 validated_part_X.txt，保证 UTF-8 BOM + 换行
    with open(validated_file, "w", encoding="utf-8-sig", newline="\n") as vf:
        vf.write("\n".join(final_rules))
    print(f"💾 validated_part_{part}.txt 已更新到: {validated_file}")

    # write_counter 统计
    counts = {i: 0 for i in range(1, WRITE_COUNTER_MAX + 1)}
    for v in counter_data.values():
        if 1 <= v <= WRITE_COUNTER_MAX:
            counts[v] += 1

    # delete_counter 统计，安全处理 KeyError
    delete_counts = Counter(delete_counter.get(r, 0) for r in final_rules)

    print("\n📊 当前分片 delete_counter 规则统计:")
    for k in sorted(delete_counts):
        print(f"    ⚠ delete_counter={k} 的规则条数: {delete_counts[k]}")

    print("\n📊 当前分片 write_counter 规则统计:")
    for i in range(1, WRITE_COUNTER_MAX + 1):
        if counts[i]:
            print(f"    ⚠ write_counter {i}/{WRITE_COUNTER_MAX} 的规则条数: {counts[i]}")

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
