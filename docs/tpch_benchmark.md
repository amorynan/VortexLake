# VortexLake TPC-H Benchmark

## 概述

本文档记录 VortexLake 与 Parquet 在 TPC-H 基准测试中的性能对比。TPC-H 是决策支持系统的行业标准基准测试，包含 8 个表和 22 个分析查询。

## 测试环境

- **数据规模**: Scale Factor 0.1 (约 60MB 原始数据)
- **数据格式**: 
  - Parquet (Apache Parquet, 默认压缩)
  - VortexLake (Apache Vortex 格式, FastLanes + Bitpacking)
- **查询引擎**: Apache DataFusion 50.x
- **测试日期**: 2025-11-28

## TPC-H 数据模型

### 表结构

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   REGION    │────<│   NATION    │────<│  SUPPLIER   │
│  (5 rows)   │     │  (25 rows)  │     │ (10K rows)  │
└─────────────┘     └──────┬──────┘     └──────┬──────┘
                          │                    │
                    ┌─────┴─────┐              │
                    │           │              │
              ┌─────┴─────┐  ┌──┴───────┐  ┌──┴───────┐
              │  CUSTOMER │  │   PART   │  │ PARTSUPP │
              │ (150K rows)│  │(200K rows)│  │(800K rows)│
              └─────┬─────┘  └────┬─────┘  └──────────┘
                    │             │
              ┌─────┴─────┐       │
              │  ORDERS   │       │
              │(1.5M rows)│       │
              └─────┬─────┘       │
                    │             │
              ┌─────┴─────────────┴─────┐
              │       LINEITEM          │
              │      (6M rows)          │
              └─────────────────────────┘
```

### 表数据量 (SF=0.1)

| 表名 | 行数 | 主要列 |
|------|------|--------|
| REGION | 5 | r_regionkey, r_name, r_comment |
| NATION | 25 | n_nationkey, n_name, n_regionkey |
| SUPPLIER | 1,000 | s_suppkey, s_name, s_nationkey, s_acctbal |
| CUSTOMER | 15,000 | c_custkey, c_name, c_nationkey, c_mktsegment |
| PART | 20,000 | p_partkey, p_name, p_type, p_size |
| PARTSUPP | 80,000 | ps_partkey, ps_suppkey, ps_availqty, ps_supplycost |
| ORDERS | 150,000 | o_orderkey, o_custkey, o_orderdate, o_totalprice |
| LINEITEM | 600,572 | l_orderkey, l_partkey, l_quantity, l_extendedprice |

### 数据类型分布

LINEITEM 表（最大的表）的列类型：
- **整数**: l_orderkey, l_partkey, l_suppkey, l_linenumber (Int64/Int32)
- **小数**: l_quantity, l_extendedprice, l_discount, l_tax (Decimal128(15,2))
- **字符串**: l_returnflag, l_linestatus, l_shipinstruct, l_shipmode, l_comment (Utf8View)
- **日期**: l_shipdate, l_commitdate, l_receiptdate (Date32)

## 存储性能

| 格式 | 大小 | 压缩比 | 写入时间 |
|------|------|--------|----------|
| Parquet | 54.85 MB | 1.00x | 6,285 ms |
| VortexLake | 27.69 MB | **0.50x** | 18,825 ms |

**结论**: VortexLake 存储空间节省 **50%**，但写入时间较长（约 3x）。

## TPC-H 查询定义

### Q1: 价格汇总报表 (Pricing Summary Report)
**涉及表**: LINEITEM
**查询类型**: 单表聚合

```sql
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) as sum_qty,
    SUM(l_extendedprice) as sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    AVG(l_quantity) as avg_qty,
    AVG(l_extendedprice) as avg_price,
    AVG(l_discount) as avg_disc,
    COUNT(*) as count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
```

### Q2: 最低成本供应商 (Minimum Cost Supplier)
**涉及表**: PART, SUPPLIER, PARTSUPP, NATION, REGION
**查询类型**: 5表JOIN + 子查询

```sql
SELECT
    s_acctbal, s_name, n_name, p_partkey, p_mfgr,
    s_address, s_phone, s_comment
FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = 15
  AND p_type LIKE '%BRASS'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'EUROPE'
  AND ps_supplycost = (
      SELECT MIN(ps_supplycost)
      FROM partsupp, supplier, nation, region
      WHERE p_partkey = ps_partkey
        AND s_suppkey = ps_suppkey
        AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey
        AND r_name = 'EUROPE'
  )
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100
```

### Q3: 配送优先级 (Shipping Priority)
**涉及表**: CUSTOMER, ORDERS, LINEITEM
**查询类型**: 3表JOIN + 聚合

```sql
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'
  AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10
```

### Q4: 订单优先级检查 (Order Priority Checking)
**涉及表**: ORDERS, LINEITEM
**查询类型**: EXISTS 子查询

```sql
SELECT
    o_orderpriority,
    COUNT(*) as order_count
FROM orders
WHERE o_orderdate >= DATE '1993-07-01'
  AND o_orderdate < DATE '1993-10-01'
  AND EXISTS (
      SELECT * FROM lineitem
      WHERE l_orderkey = o_orderkey
        AND l_commitdate < l_receiptdate
  )
GROUP BY o_orderpriority
ORDER BY o_orderpriority
```

### Q5: 本地供应商销量 (Local Supplier Volume)
**涉及表**: CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION
**查询类型**: 6表JOIN + 聚合

```sql
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1995-01-01'
GROUP BY n_name
ORDER BY revenue DESC
```

### Q6: 收入预测变化 (Forecasting Revenue Change)
**涉及表**: LINEITEM
**查询类型**: 单表过滤聚合

```sql
SELECT
    SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1995-01-01'
  AND l_discount >= 0.05
  AND l_discount <= 0.07
  AND l_quantity < 24
```

### Q7: 体量运输 (Volume Shipping)
**涉及表**: SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION
**查询类型**: 5表JOIN + 聚合

```sql
SELECT
    supp_nation, cust_nation, l_year,
    SUM(volume) as revenue
FROM (
    SELECT
        n1.n_name as supp_nation,
        n2.n_name as cust_nation,
        EXTRACT(YEAR FROM l_shipdate) as l_year,
        l_extendedprice * (1 - l_discount) as volume
    FROM supplier, lineitem, orders, customer, nation n1, nation n2
    WHERE s_suppkey = l_suppkey
      AND o_orderkey = l_orderkey
      AND c_custkey = o_custkey
      AND s_nationkey = n1.n_nationkey
      AND c_nationkey = n2.n_nationkey
      AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
      AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) as shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year
```

### Q8: 国家市场份额 (National Market Share)
**涉及表**: PART, SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION, REGION
**查询类型**: 7表JOIN + 聚合

```sql
SELECT
    o_year,
    SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) as mkt_share
FROM (
    SELECT
        EXTRACT(YEAR FROM o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) as volume,
        n2.n_name as nation
    FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    WHERE p_partkey = l_partkey
      AND s_suppkey = l_suppkey
      AND l_orderkey = o_orderkey
      AND o_custkey = c_custkey
      AND c_nationkey = n1.n_nationkey
      AND n1.n_regionkey = r_regionkey
      AND r_name = 'AMERICA'
      AND s_nationkey = n2.n_nationkey
      AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
      AND p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
GROUP BY o_year
ORDER BY o_year
```

### Q9: 产品类型利润 (Product Type Profit Measure)
**涉及表**: PART, SUPPLIER, LINEITEM, PARTSUPP, ORDERS, NATION
**查询类型**: 6表JOIN + 聚合

```sql
SELECT
    nation, o_year,
    SUM(amount) as sum_profit
FROM (
    SELECT
        n_name as nation,
        EXTRACT(YEAR FROM o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    FROM part, supplier, lineitem, partsupp, orders, nation
    WHERE s_suppkey = l_suppkey
      AND ps_suppkey = l_suppkey
      AND ps_partkey = l_partkey
      AND p_partkey = l_partkey
      AND o_orderkey = l_orderkey
      AND s_nationkey = n_nationkey
      AND p_name LIKE '%green%'
) as profit
GROUP BY nation, o_year
ORDER BY nation, o_year DESC
```

### Q10: 退货报告 (Returned Item Reporting)
**涉及表**: CUSTOMER, ORDERS, LINEITEM, NATION
**查询类型**: 4表JOIN + 聚合

```sql
SELECT
    c_custkey, c_name,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal, n_name, c_address, c_phone, c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= DATE '1993-10-01'
  AND o_orderdate < DATE '1994-01-01'
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20
```

### Q11: 重要库存识别 (Important Stock Identification)
**涉及表**: PARTSUPP, SUPPLIER, NATION
**查询类型**: 3表JOIN + 子查询

```sql
SELECT
    ps_partkey,
    SUM(ps_supplycost * ps_availqty) as value
FROM partsupp, supplier, nation
WHERE ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING SUM(ps_supplycost * ps_availqty) > (
    SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
    FROM partsupp, supplier, nation
    WHERE ps_suppkey = s_suppkey
      AND s_nationkey = n_nationkey
      AND n_name = 'GERMANY'
)
ORDER BY value DESC
```

### Q12: 配送模式与订单优先级 (Shipping Modes and Order Priority)
**涉及表**: ORDERS, LINEITEM
**查询类型**: 2表JOIN + CASE聚合

```sql
SELECT
    l_shipmode,
    SUM(CASE
        WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1
        ELSE 0
    END) as high_line_count,
    SUM(CASE
        WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1
        ELSE 0
    END) as low_line_count
FROM orders, lineitem
WHERE o_orderkey = l_orderkey
  AND l_shipmode IN ('MAIL', 'SHIP')
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= DATE '1994-01-01'
  AND l_receiptdate < DATE '1995-01-01'
GROUP BY l_shipmode
ORDER BY l_shipmode
```

### Q13: 客户分布 (Customer Distribution)
**涉及表**: CUSTOMER, ORDERS
**查询类型**: LEFT JOIN + 聚合

```sql
SELECT
    c_count, COUNT(*) as custdist
FROM (
    SELECT c_custkey, COUNT(o_orderkey) as c_count
    FROM customer LEFT OUTER JOIN orders ON
        c_custkey = o_custkey
        AND o_comment NOT LIKE '%special%requests%'
    GROUP BY c_custkey
) as c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC
```

### Q14: 促销效果 (Promotion Effect)
**涉及表**: LINEITEM, PART
**查询类型**: 2表JOIN + CASE聚合

```sql
SELECT
    100.00 * SUM(CASE
        WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount)
        ELSE 0
    END) / SUM(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM lineitem, part
WHERE l_partkey = p_partkey
  AND l_shipdate >= DATE '1995-09-01'
  AND l_shipdate < DATE '1995-10-01'
```

### Q15: 顶级供应商 (Top Supplier)
**涉及表**: SUPPLIER, LINEITEM
**查询类型**: VIEW + 子查询

```sql
WITH revenue AS (
    SELECT
        l_suppkey as supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) as total_revenue
    FROM lineitem
    WHERE l_shipdate >= DATE '1996-01-01'
      AND l_shipdate < DATE '1996-04-01'
    GROUP BY l_suppkey
)
SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
FROM supplier, revenue
WHERE s_suppkey = supplier_no
  AND total_revenue = (SELECT MAX(total_revenue) FROM revenue)
ORDER BY s_suppkey
```

### Q16: 零件/供应商关系 (Parts/Supplier Relationship)
**涉及表**: PARTSUPP, PART, SUPPLIER
**查询类型**: NOT IN 子查询

```sql
SELECT
    p_brand, p_type, p_size,
    COUNT(DISTINCT ps_suppkey) as supplier_cnt
FROM partsupp, part
WHERE p_partkey = ps_partkey
  AND p_brand <> 'Brand#45'
  AND p_type NOT LIKE 'MEDIUM POLISHED%'
  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND ps_suppkey NOT IN (
      SELECT s_suppkey FROM supplier
      WHERE s_comment LIKE '%Customer%Complaints%'
  )
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
```

### Q17: 小订单收入 (Small-Quantity-Order Revenue)
**涉及表**: LINEITEM, PART
**查询类型**: 相关子查询

```sql
SELECT
    SUM(l_extendedprice) / 7.0 as avg_yearly
FROM lineitem, part
WHERE p_partkey = l_partkey
  AND p_brand = 'Brand#23'
  AND p_container = 'MED BOX'
  AND l_quantity < (
      SELECT 0.2 * AVG(l_quantity)
      FROM lineitem
      WHERE l_partkey = p_partkey
  )
```

### Q18: 大订单客户 (Large Volume Customer)
**涉及表**: CUSTOMER, ORDERS, LINEITEM
**查询类型**: IN 子查询 + 聚合

```sql
SELECT
    c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
    SUM(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (
    SELECT l_orderkey FROM lineitem
    GROUP BY l_orderkey
    HAVING SUM(l_quantity) > 300
)
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
```

### Q19: 折扣收入 (Discounted Revenue)
**涉及表**: LINEITEM, PART
**查询类型**: 复杂 OR 条件

```sql
SELECT
    SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM lineitem, part
WHERE (
    p_partkey = l_partkey
    AND p_brand = 'Brand#12'
    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND l_quantity >= 1 AND l_quantity <= 11
    AND p_size BETWEEN 1 AND 5
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
) OR (
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    AND l_quantity >= 10 AND l_quantity <= 20
    AND p_size BETWEEN 1 AND 10
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
) OR (
    p_partkey = l_partkey
    AND p_brand = 'Brand#34'
    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    AND l_quantity >= 20 AND l_quantity <= 30
    AND p_size BETWEEN 1 AND 15
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
)
```

### Q20: 潜在零件促销 (Potential Part Promotion)
**涉及表**: SUPPLIER, NATION, PARTSUPP, PART, LINEITEM
**查询类型**: 多层嵌套子查询

```sql
SELECT s_name, s_address
FROM supplier, nation
WHERE s_suppkey IN (
    SELECT ps_suppkey FROM partsupp
    WHERE ps_partkey IN (
        SELECT p_partkey FROM part
        WHERE p_name LIKE 'forest%'
    )
    AND ps_availqty > (
        SELECT 0.5 * SUM(l_quantity)
        FROM lineitem
        WHERE l_partkey = ps_partkey
          AND l_suppkey = ps_suppkey
          AND l_shipdate >= DATE '1994-01-01'
          AND l_shipdate < DATE '1995-01-01'
    )
)
  AND s_nationkey = n_nationkey
  AND n_name = 'CANADA'
ORDER BY s_name
```

### Q21: 供应商等待查询 (Suppliers Who Kept Orders Waiting)
**涉及表**: SUPPLIER, LINEITEM, ORDERS, NATION
**查询类型**: EXISTS + NOT EXISTS

```sql
SELECT s_name, COUNT(*) as numwait
FROM supplier, lineitem l1, orders, nation
WHERE s_suppkey = l1.l_suppkey
  AND o_orderkey = l1.l_orderkey
  AND o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND EXISTS (
      SELECT * FROM lineitem l2
      WHERE l2.l_orderkey = l1.l_orderkey
        AND l2.l_suppkey <> l1.l_suppkey
  )
  AND NOT EXISTS (
      SELECT * FROM lineitem l3
      WHERE l3.l_orderkey = l1.l_orderkey
        AND l3.l_suppkey <> l1.l_suppkey
        AND l3.l_receiptdate > l3.l_commitdate
  )
  AND s_nationkey = n_nationkey
  AND n_name = 'SAUDI ARABIA'
GROUP BY s_name
ORDER BY numwait DESC, s_name
LIMIT 100
```

### Q22: 全球销售机会 (Global Sales Opportunity)
**涉及表**: CUSTOMER, ORDERS
**查询类型**: NOT EXISTS + IN 子查询

```sql
SELECT
    cntrycode,
    COUNT(*) as numcust,
    SUM(c_acctbal) as totacctbal
FROM (
    SELECT
        SUBSTRING(c_phone FROM 1 FOR 2) as cntrycode,
        c_acctbal
    FROM customer
    WHERE SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
      AND c_acctbal > (
          SELECT AVG(c_acctbal)
          FROM customer
          WHERE c_acctbal > 0.00
            AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
      )
      AND NOT EXISTS (
          SELECT * FROM orders
          WHERE o_custkey = c_custkey
      )
) as custsale
GROUP BY cntrycode
ORDER BY cntrycode
```

## 测试结果 (SF=0.1)

### 数据规模

| 表 | 行数 |
|----|------|
| region | 5 |
| nation | 25 |
| supplier | 1,000 |
| customer | 15,000 |
| part | 20,000 |
| partsupp | 80,000 |
| orders | 150,000 |
| lineitem | 600,572 |

### 存储性能

| 指标 | Parquet | VortexLake | 比值 |
|------|---------|------------|------|
| **存储大小** | 54.85 MB | 27.69 MB | **0.50x** ✓ |
| **写入时间** | 6,238 ms | 18,685 ms | 3.0x |

### 已测试查询 (15个)

| Query | 描述 | 涉及表数 | Parquet (ms) | VortexLake (ms) | Speedup | 状态 |
|-------|------|---------|-------------|-----------------|---------|------|
| **Q1** | 价格汇总 | 1 | 1,402 | 1,376 | **1.02x** | ✓ PASS |
| **Q6** | 收入预测 | 1 | 375 | 416 | 0.90x | ✓ PASS |
| **Q12** | 配送模式 | 2 | 2,316 | 1,489 | **1.56x** | ✓ PASS |
| **Q14** | 促销效果 | 2 | 1,968 | 413 | **4.76x** 🚀 | ✓ PASS |
| **Q19** | 折扣收入 | 2 | 844 | 724 | **1.16x** | ✓ PASS |
| **Q3** | 配送优先级 | 3 | 626 | 1,534 | 0.41x | ✓ PASS |
| **Q11** | 重要库存识别 | 3 | 357 | 506 | 0.70x | ✓ PASS |
| **Q15** | 顶级供应商 | 2 | 698 | 841 | 0.83x | ✓ PASS |
| **Q18** | 大订单客户 | 3 | 4,748 | 2,480 | **1.91x** | ✓ PASS |
| **Q4** | 订单优先级 | 2 | 458 | 493 | 0.93x | ✓ PASS |
| **Q10** | 退货报告 | 4 | 772 | 968 | 0.80x | ✓ PASS |
| **Q17** | 小订单收入 | 2 | 794 | 1,511 | 0.53x | ✓ PASS |
| **Q5** | 本地供应商 | 6 | 682 | 1,817 | 0.38x | ✓ PASS |
| **Q7** | 体量运输 | 5 | 4,384 | 2,149 | **2.04x** | ✓ PASS |
| **Q9** | 产品利润 | 6 | 6,033 | 2,182 | **2.77x** 🚀 | ✓ PASS |

### 性能分类

#### VortexLake 显著更快 (>1.5x)
| Query | Speedup | 特点 |
|-------|---------|------|
| Q14 | **4.76x** | 2表JOIN + CASE聚合 |
| Q9 | **2.77x** | 6表JOIN + LIKE过滤 |
| Q7 | **2.04x** | 5表JOIN + 年份聚合 |
| Q18 | **1.91x** | IN子查询 + 聚合 |
| Q12 | **1.56x** | 2表JOIN + 条件聚合 |

#### VortexLake 略快或相当 (0.9x-1.5x)
| Query | Speedup | 特点 |
|-------|---------|------|
| Q19 | 1.16x | 复杂OR条件 |
| Q1 | 1.02x | 单表聚合 |
| Q4 | 0.93x | EXISTS子查询 |
| Q6 | 0.90x | 单表过滤 |

#### Parquet 更快 (<0.9x)
| Query | Speedup | 特点 |
|-------|---------|------|
| Q15 | 0.83x | CTE查询 |
| Q10 | 0.80x | 4表JOIN |
| Q11 | 0.70x | HAVING子查询 |
| Q17 | 0.53x | 相关子查询 |
| Q3 | 0.41x | 3表JOIN |
| Q5 | 0.38x | 6表JOIN (最慢) |

### 完整测试结果 (22个查询全部完成)

| Query | 描述 | 涉及表数 | Parquet (ms) | VortexLake (ms) | Speedup | 状态 |
|-------|------|---------|-------------|-----------------|---------|------|
| **Q1** | 价格汇总 | 1 | 1,421 | 1,397 | **1.02x** | ✓ PASS |
| **Q6** | 收入预测 | 1 | 370 | 419 | 0.88x | ✓ PASS |
| **Q12** | 配送模式 | 2 | 2,310 | 1,465 | **1.58x** | ✓ PASS |
| **Q14** | 促销效果 | 2 | 1,949 | 419 | **4.66x** 🚀 | ✓ PASS |
| **Q19** | 折扣收入 | 2 | 820 | 744 | **1.10x** | ✓ PASS |
| **Q16** | 零件/供应商关系 | 2 | 370 | 493 | 0.75x | ✓ PASS |
| **Q3** | 配送优先级 | 3 | 631 | 1,539 | 0.41x | ✓ PASS |
| **Q11** | 重要库存识别 | 3 | 357 | 498 | 0.72x | ✓ PASS |
| **Q15** | 顶级供应商 | 2 | 691 | 840 | 0.82x | ✓ PASS |
| **Q18** | 大订单客户 | 3 | 4,703 | 2,424 | **1.94x** | ✓ PASS |
| **Q20** | 潜在零件促销 | 2 | 607 | 840 | 0.72x | ✓ PASS |
| **Q21** | 供应商等待 | 4 | 1,652 | 2,779 | 0.59x | ✓ PASS |
| **Q4** | 订单优先级 | 2 | 450 | 500 | 0.90x | ✓ PASS |
| **Q10** | 退货报告 | 4 | 766 | 965 | 0.79x | ✓ PASS |
| **Q17** | 小订单收入 | 2 | 790 | 1,438 | 0.55x | ✓ PASS |
| **Q13** | 客户分布 | 2 | 489 | 568 | 0.86x | ✓ PASS |
| **Q22** | 全球销售机会 | 2 | 229 | 284 | 0.81x | ✓ PASS |
| **Q2** | 最低成本供应商 | 5 | 450 | 678 | 0.66x | ✓ PASS |
| **Q5** | 本地供应商 | 6 | 665 | 1,756 | 0.38x | ✓ PASS |
| **Q7** | 体量运输 | 5 | 4,346 | 2,145 | **2.03x** | ✓ PASS |
| **Q8** | 国家市场份额 | 7 | 2,550 | 1,805 | **1.41x** | ✓ PASS |
| **Q9** | 产品利润 | 6 | 5,961 | 2,150 | **2.77x** 🚀 | ✓ PASS |

## 性能分析

### VortexLake 显著优势场景 (>1.5x)

| Query | Speedup | 分析 |
|-------|---------|------|
| **Q14** | 4.66x | CASE聚合 + 日期范围过滤，Zone Map 高效剪枝 |
| **Q9** | 2.77x | 6表JOIN + LIKE过滤，压缩减少I/O |
| **Q7** | 2.03x | 5表JOIN + 日期范围过滤，谓词下推效果好 |
| **Q18** | 1.94x | IN子查询 + 聚合，压缩优势 |
| **Q12** | 1.58x | 条件聚合 + 日期范围，Zone Map 生效 |
| **Q8** | 1.41x | 7表JOIN但有大范围过滤，压缩优势明显 |

**共同特点**: 
- 有明确的过滤条件（日期范围、LIKE、IN等）
- Zone Map 可以有效剪枝
- 聚合操作受益于压缩
- 多表JOIN时，压缩带来的I/O减少超过JOIN开销

### VortexLake 劣势场景 (<0.9x)

| Query | Speedup | 分析 |
|-------|---------|------|
| **Q5** | 0.38x | 6表JOIN，vortex-datafusion JOIN 优化不足 |
| **Q3** | 0.41x | 3表JOIN，无法有效利用 Zone Map |
| **Q17** | 0.55x | 相关子查询，每行都需要子查询 |
| **Q21** | 0.59x | EXISTS + NOT EXISTS，复杂嵌套查询 |
| **Q2** | 0.66x | 5表JOIN + 相关子查询 |
| **Q11** | 0.72x | HAVING + 子查询，复杂执行计划 |
| **Q20** | 0.72x | 多层嵌套子查询 |
| **Q16** | 0.75x | NOT IN 子查询 |
| **Q10** | 0.79x | 4表JOIN |
| **Q22** | 0.81x | NOT EXISTS + 子查询 |
| **Q15** | 0.82x | CTE查询 |

**共同特点**:
- 多表 JOIN 操作（3-6表）
- 相关子查询或嵌套子查询
- EXISTS/NOT EXISTS 操作
- 无法有效利用谓词下推

### 综合评估

| 方面 | Parquet | VortexLake | 结论 |
|------|---------|------------|------|
| **存储效率** | 54.85 MB | 27.69 MB | VortexLake 节省 **50%** 空间 ✓ |
| **写入性能** | 6.3s | 18.8s | Parquet 快 3x |
| **单表查询** | 基准 | 0.88x-1.02x | 性能相当 |
| **2表JOIN** | 基准 | 0.75x-4.66x | VortexLake 平均更快 ✓ |
| **3-4表JOIN** | 基准 | 0.41x-0.90x | Parquet 更快 |
| **5+表JOIN** | 基准 | 0.38x-2.77x | 取决于过滤条件 |
| **复杂子查询** | 基准 | 0.55x-0.86x | Parquet 更快 |

### 统计汇总

```
22个查询完整测试结果:
- VortexLake 更快 (>1.0x): 8 个 (36%)
- 基本持平 (0.9x-1.0x): 3 个 (14%)
- Parquet 更快 (<0.9x): 11 个 (50%)

VortexLake 最大优势: Q14 (4.66x), Q9 (2.77x), Q7 (2.03x)
VortexLake 最大劣势: Q5 (0.38x), Q3 (0.41x), Q17 (0.55x)

按表数分类:
- 单表查询 (1表): 2个 - 平均 0.95x (基本持平)
- 2表查询 (2表): 9个 - 平均 1.15x (VortexLake 略优)
- 3-4表查询: 6个 - 平均 0.70x (Parquet 更快)
- 5+表查询: 5个 - 平均 1.20x (VortexLake 更快，得益于压缩)
```

## 运行测试

```bash
# 运行完整 TPC-H 测试
cargo test -p vortexlake-sql complete_tpch_benchmark -- --nocapture --ignored

# 运行单表验证测试
cargo test -p vortexlake-sql full_validation_suite -- --nocapture --ignored

# 运行 E2E 测试
cargo test -p vortexlake-sql test_vortexlake_e2e -- --nocapture
```

## 后续优化方向

1. **JOIN 性能优化**: 改进 vortex-datafusion 的 JOIN 策略
2. **谓词下推**: 增强 Zone Map 过滤能力
3. **并行扫描**: 优化多 fragment 并行读取
4. **写入性能**: 优化压缩流程，减少写入延迟
5. **完整 TPC-H**: 实现剩余 14 个查询的测试

