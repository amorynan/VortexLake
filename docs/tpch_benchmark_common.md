# TPC-H Benchmark 通用定义

本文档包含 TPC-H 基准测试的数据模型、表结构、22 个查询 SQL 以及运行方式，供各日期版本的 benchmark 报告引用。

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

## 运行测试

```bash
# 运行完整 TPC-H 测试
cargo test -p vortexlake-sql complete_tpch_benchmark -- --nocapture --ignored

# 运行单表验证测试
cargo test -p vortexlake-sql full_validation_suite -- --nocapture --ignored

# 运行 E2E 测试
cargo test -p vortexlake-sql test_vortexlake_e2e -- --nocapture
```
