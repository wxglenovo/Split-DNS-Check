DNS 验证 & 分片规则管理脚本

该脚本用于验证分片化的广告/过滤规则，对规则进行 DNS 验证，并维护 validated_part_X.txt 与 not_written_counter.json。
它支持自动删除连续无效的规则，并统计验证、删除、过滤情况。

功能

DNS 验证

对 tmp/part_X.txt 中的规则进行 DNS 查询验证

验证成功的规则写入 tmp/vpart_X.tmp

更新 JSON 和分片文件

dist/not_written_counter.json 保存每条规则的 write_counter

dist/validated_part_X.txt 保存验证成功的规则

连续无效的规则 (write_counter ≤ 0) 会被删除

统计输出

显示分片总数、新增数、删除数、过滤数

删除规则前 20 条会打印日志

project/
├─ dist/
│  ├─ validated_part_1.txt
│  └─ not_written_counter.json
├─ tmp/
│  ├─ part_1.txt
│  └─ vpart_1.tmp
├─ split_and_check_16.py   <-- 脚本文件
└─ urls.txt                <-- 可选，规则源


tmp/part_X.txt ：原始分片规则

tmp/vpart_X.tmp ：DNS 验证成功规则

dist/validated_part_X.txt ：经过验证、更新的规则

dist/not_written_counter.json ：规则的 write_counter 状态

使用方法
1. 安装依赖
pip install dnspython

2. 执行脚本
python split_and_check_16.py 6


6 表示验证第 6 分片

可以依次执行 1~16 分片，或者在 CI/CD 中并行执行

3. 输出示例
✅ 分片 6 完成: 总116768, 新增4821, 删除116768, 过滤1234
COMMIT_STATS:总116768,新增4821,删除116768,过滤1234


总数：最终有效规则数

新增：DNS 验证成功的规则

删除：从 validated_part_X.txt 中删除的规则

过滤：原规则存在但 DNS 验证失败的规则数

4. 日志

删除规则前 20 条会打印：

💥 write_counter ≤ 3 → 从 JSON 删除：||www.example.com^
...
🗑 本次从 JSON 删除 共 116768 条规则

JSON 文件结构

dist/not_written_counter.json 示例：

{
  "validated_part_1": {
    "||example.com^": 4,
    "||abc.com^": 3
  },
  "validated_part_2": { ... }
}


write_counter ：记录规则连续失效次数

当 write_counter ≤ 0 时，规则会从 validated_part_X.txt 和 JSON 中删除

注意事项

确保 tmp/part_X.txt 文件存在，否则无法验证

脚本会覆盖 dist/validated_part_X.txt，请做好备份

确保脚本有写入 dist/ 目录权限

DNS 查询较多时，可调整线程数 ThreadPoolExecutor(max_workers=50)
