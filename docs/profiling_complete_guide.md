# 完整 Profiling 工具使用指南

## 概述

VortexLake 提供了一个**完整的查询性能分析工具**，可以深入分析查询的每个阶段和操作符的性能。

## 功能特性

### 1. 多阶段性能分析

测量查询执行的各个阶段：
- **Parse SQL**: SQL 解析时间
- **Optimize Logical Plan**: 逻辑计划优化时间  
- **Create Physical Plan**: 物理计划创建时间
- **Execute Query**: 实际执行时间

每个阶段显示：
- 绝对时间（毫秒）
- 占总时间的百分比
- 累积时间

### 2. 执行计划树分析

显示完整的执行计划树形结构：
- 每个操作符的类型和名称
- 操作符之间的父子关系
- 每个操作符的 metrics（行数、时间、I/O 等）

### 3. 操作符级 Metrics

为每个操作符收集：
- **output_rows**: 输出的行数
- **spilled_bytes**: 溢出到磁盘的字节数
- **spilled_rows**: 溢出到磁盘的行数
- **elapsed_compute**: CPU 计算时间（如果可用）

### 4. 瓶颈识别

自动识别：
- 最慢的操作符
- 处理最多数据的操作符
- 计划深度和复杂度

### 5. 对比分析

支持两个查询 profile 的详细对比：
- 各阶段时间对比
- 操作符数量对比
- 计划深度对比
- 性能差异百分比

## 使用方法

### 基本用法

```rust
use vortexlake_sql::profiling::execute_with_full_profile;

let (results, profile) = execute_with_full_profile(&ctx, sql).await?;
profile.print();  // 打印完整的 profile 报告
```

### 对比两个查询

```rust
use vortexlake_sql::profiling::{execute_with_full_profile, compare_profiles};

let (results1, profile1) = execute_with_full_profile(&parquet_ctx, sql).await?;
let (results2, profile2) = execute_with_full_profile(&vortex_ctx, sql).await?;

compare_profiles(&profile1, "Parquet", &profile2, "VortexLake");
```

### 导出为文本文件

```rust
use vortexlake_sql::profiling::{execute_with_full_profile, export_profile_to_file};

let (results, profile) = execute_with_full_profile(&ctx, sql).await?;
export_profile_to_file(&profile, "profile_report.txt")?;
```

导出的文本文件包含：
- 执行阶段分析（时间、百分比）
- 执行计划树（操作符名称和 metrics）
- Profile 摘要（总体统计）

### 导出为 JSON

```rust
use vortexlake_sql::profiling::{execute_with_full_profile, export_profile_json};

let (results, profile) = execute_with_full_profile(&ctx, sql).await?;
let json = export_profile_json(&profile);
println!("{}", serde_json::to_string_pretty(&json)?);
```

## 运行 Q22 Profiling 测试

### 准备测试数据

```bash
# 首先运行完整 TPC-H 测试生成数据
cargo test -p vortexlake-sql complete_tpch_benchmark -- --nocapture --ignored
```

### 运行 Profiling

```bash
cargo test -p vortexlake-sql profile_q22 -- --nocapture --ignored
```

## 输出示例

### 执行阶段分析

```
================================================================================
Execution Phases
================================================================================
Phase                                    Time (ms)  Percentage  Cumulative
--------------------------------------------------------------------------------
1. Parse SQL                                 1.23        0.5%        0.5%
2. Optimize Logical Plan                    12.45        5.4%        5.9%
3. Create Physical Plan                     18.67        8.2%       14.1%
4. Execute Query                           196.65       85.9%      100.0%
--------------------------------------------------------------------------------
TOTAL                                     229.00      100.0%      100.0%
```

### 执行计划树

```
================================================================================
Execution Plan Tree
================================================================================
└── ProjectionExec [rows:100]
    └── AggregateExec [rows:100, time:45.23ms]
        ├── HashJoinExec [rows:5000, time:120.45ms]
        │   ├── FilterExec [rows:5000, time:15.67ms]
        │   │   └── ParquetExec [rows:15000, time:80.12ms]
        │   └── FilterExec [rows:10000, time:20.34ms]
        │       └── VortexExec [rows:10000, time:95.23ms]
        └── ProjectionExec [rows:5000]
            └── FilterExec [rows:5000]
                └── ParquetExec [rows:15000]
```

### Profile 摘要

```
================================================================================
Profile Summary
================================================================================
Total Execution Time:     229.00 ms
Total Operators:           12
Plan Depth:                 5
Total Rows Processed:    15000
Slowest Operator:          HashJoinExec
Largest Operator (rows):   ParquetExec
```

### 对比分析

```
====================================================================================================
Profile Comparison: Parquet vs VortexLake
====================================================================================================

--- Phase Comparison ---
Phase                                    Parquet (ms)  VortexLake (ms)          Ratio    Difference
----------------------------------------------------------------------------------------------------
1. Parse SQL                                    1.23             1.45           0.85x        -0.22 ms (-15.2%)
2. Optimize Logical Plan                       12.45            15.23           0.82x        -2.78 ms (-18.3%)
3. Create Physical Plan                        18.67            32.12           0.58x       -13.45 ms (-41.9%)
4. Execute Query                              196.65           235.20           0.84x       -38.55 ms (-16.4%)

--- Summary Comparison ---
Metric                                    Parquet  VortexLake          Ratio
---------------------------------------------------------------------------------------
Total Time (ms)                           229.00       284.00           0.81x
Operator Count                                 12            12           1.00x
Plan Depth                                       5             5           1.00x
Rows Processed                             15000         15000           1.00x
```

## 分析 Q22 性能瓶颈

### 从 Profile 中识别问题

运行 `profile_q22` 后，查看输出：

1. **Execute Query 阶段慢**
   - 如果 Execute Query 占 80%+ 时间，这是正常的
   - 如果其他阶段占比高，说明优化有问题

2. **Create Physical Plan 慢**（如 Q22）
   ```
   Create Physical Plan: 32.12 ms (11.3%)  ← 比 Parquet 慢 72%
   ```
   - 可能原因：
     - 复杂的子查询展开
     - NOT EXISTS 转换为 JOIN 的复杂度
     - 字符串函数处理

3. **执行计划树中的瓶颈**
   ```
   └── FilterExec [rows:15000, time:180.23ms]  ← 最慢的操作符
       └── ProjectionExec [rows:15000]
           └── ParquetExec [rows:15000, time:95.12ms]
   ```
   - FilterExec 慢：可能是 SUBSTRING 函数开销
   - 查看哪个操作符处理最多数据

### Q22 具体分析步骤

1. **运行 profiling**:
   ```bash
   cargo test -p vortexlake-sql profile_q22 -- --nocapture --ignored
   ```

2. **查看阶段对比**:
   - 找出哪个阶段 VortexLake 明显慢于 Parquet
   - Q22 预期：Create Physical Plan 和 Execute Query 都会慢

3. **查看执行计划树**:
   - 找出处理最多数据的操作符
   - 找出最慢的操作符
   - 检查是否有不必要的嵌套

4. **分析操作符 metrics**:
   - 如果某个 FilterExec 处理 15,000 行但很慢 → SUBSTRING 瓶颈
   - 如果 HashJoinExec 很慢 → JOIN 算法问题
   - 如果 spilled_bytes > 0 → 内存不足，需要优化

## 高级功能

### 1. 自定义 Metrics 收集

可以在 `VortexLakeTableProvider` 中添加自定义 metrics：

```rust
impl VortexLakeTableProvider {
    fn scan_with_custom_metrics(&self, ...) -> Result<...> {
        let metrics = BaselineMetrics::new(&plan_metrics, partition);
        let start = Instant::now();
        
        // ... 执行扫描
        
        metrics.elapsed_compute().add(start.elapsed());
        metrics.record_output(rows);
    }
}
```

### 2. 结合 EXPLAIN 分析

```rust
// 获取执行计划
let explain = ctx.sql("EXPLAIN SELECT ...").await?;
let plan_results = explain.collect().await?;

// 结合 profile 分析
let (results, profile) = execute_with_full_profile(&ctx, sql).await?;
// 对比计划结构和实际执行时间
```

### 3. 导出 JSON 进行进一步分析

```rust
let json = export_profile_json(&profile);
std::fs::write("profile.json", serde_json::to_string_pretty(&json)?)?;

// 可以用 Python/JavaScript 进行可视化
```

## 性能优化建议

基于 profiling 结果：

### 如果 Create Physical Plan 慢

1. **简化查询**：减少子查询嵌套
2. **优化子查询**：将相关子查询改为 JOIN
3. **预计算**：使用物化视图或计算列

### 如果 Execute Query 慢

1. **操作符级别优化**：
   - 如果 FilterExec 慢 → 优化谓词下推
   - 如果 HashJoinExec 慢 → 优化 JOIN 算法
   - 如果 ProjectionExec 慢 → 减少投影列

2. **I/O 优化**：
   - 如果 spilled_bytes > 0 → 增加内存或优化数据分布
   - 如果扫描慢 → 优化 Zone Map 过滤

3. **字符串操作优化**：
   - 如果 SUBSTRING 慢 → 预计算或使用计算列
   - 优化字符串压缩/解压

## 工具对比

| 功能 | 简单版本 | 完整版本 |
|------|---------|---------|
| 阶段计时 | ✅ | ✅ |
| 执行计划树 | ❌ | ✅ |
| 操作符 metrics | ❌ | ✅ |
| 瓶颈识别 | ❌ | ✅ |
| 对比分析 | 基础 | 详细 |
| JSON 导出 | ❌ | ✅ |
| 可视化输出 | ❌ | ✅ |

## 总结

完整的 profiling 工具提供了：

1. ✅ **详细的阶段分析** - 知道时间花在哪里
2. ✅ **执行计划树** - 理解查询结构
3. ✅ **操作符级 metrics** - 找出具体瓶颈
4. ✅ **自动瓶颈识别** - 快速定位问题
5. ✅ **对比分析** - Parquet vs VortexLake
6. ✅ **JSON 导出** - 进一步分析和可视化

**使用完整 profiling 工具，可以精确识别 Q22 的性能瓶颈！**

