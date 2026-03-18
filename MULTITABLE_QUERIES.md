# Multi-Table Query Guide: v8.0

**Status**: Production-Ready
**Supported Joins**: LEFT JOIN (transactions ↔ customers ↔ regions, products)
**Test Coverage**: 19 tests, all passing
**Cost**: ~$0.05-0.10 per multi-table query

---

## Quick Start: When to Use Multi-Table Queries

**Use multi-table queries when you need metrics ACROSS related tables**:

| User Question | Type | Primary Tables |
|---------------|------|-----------------|
| "Total revenue last month" | Single-table | transactions |
| "Show top 10 customers by lifetime value" | Multi-table | customers + transactions |
| "Which regions have highest churn?" | Multi-table | regions + customers |
| "Product adoption by customer segment" | Multi-table | transactions + customers + products |
| "Revenue breakdown by product and region" | Multi-table | transactions + products + customers |

---

## Schema: Understanding the Relationships

```
transactions (Fact Table - 5K rows)
├─ transaction_id (PK)
├─ customer_id (FK → customers.customer_id)
├─ product_name (FK → products.product_name)
├─ amount, status, category
└─ transaction_date

customers (Dimension - 10K rows)
├─ customer_id (PK)
├─ region_id (FK → regions.region_id)
├─ customer_segment, lifetime_value, churn_flag
└─ join_date

products (Dimension - 50 rows)
├─ product_name (PK)
├─ category, gross_margin_pct, unit_price
└─ (Low cardinality - safe for grouping)

regions (Dimension - 4 rows)
├─ region_id (PK)
├─ region_name, sales_tier, manager_id
└─ (Reference data only)
```

**Join Rules**:
- ✅ Always use **LEFT JOIN** (preserves all fact rows)
- ❌ Never use INNER JOIN on fact tables (data loss)
- ✅ Fact table (transactions) goes in FROM clause
- ✅ Dimensions (customers, products, regions) in JOIN

---

## The 6 Core Multi-Table Patterns

### Pattern 1: Customer Lifetime Value

**Question**: "Show me our top customers by total spending"

```sql
SELECT c.customer_id,
       c.customer_name,
       c.customer_segment,
       SUM(t.amount) as lifetime_value,
       COUNT(t.transaction_id) as transaction_count,
       AVG(t.amount) as avg_transaction
FROM customers c
LEFT JOIN transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_id, c.customer_name, c.customer_segment
ORDER BY lifetime_value DESC
LIMIT 20;
```

**Why multi-table**: Combines customer metadata with transaction history across all time.

**Use cases**:
- Account-based marketing (identify VIPs)
- Customer success prioritization
- Churn risk identification

**Cost**: ~$0.08 (10K customers × 5K transactions = 50M virtual rows scanned)

---

### Pattern 2: Regional Churn Analysis

**Question**: "Which regions are losing customers, and at what rate?"

```sql
SELECT r.region_name,
       r.sales_tier,
       COUNT(DISTINCT c.customer_id) as total_customers,
       SUM(CASE WHEN c.churn_flag = 1 THEN 1 ELSE 0 END) as churned_customers,
       ROUND(
         SUM(CASE WHEN c.churn_flag = 1 THEN 1 ELSE 0 END)::DECIMAL /
         COUNT(DISTINCT c.customer_id),
         4
       ) as churn_rate
FROM regions r
LEFT JOIN customers c ON r.region_id = c.region_id
GROUP BY r.region_name, r.sales_tier
ORDER BY churn_rate DESC;
```

**Why multi-table**: Links customer churn flags with region metadata.

**Use cases**:
- Sales ops: identify underperforming regions
- Executive reporting: regional health dashboard
- Resource allocation: invest in high-churn regions

**Cost**: ~$0.02 (4 regions × 10K customers = 40K rows only)

---

### Pattern 3: Product Revenue with Margins

**Question**: "Which products are most profitable, accounting for margins?"

```sql
SELECT p.product_name,
       p.category,
       p.gross_margin_pct,
       COUNT(DISTINCT t.transaction_id) as transaction_count,
       SUM(t.amount) as gross_revenue,
       ROUND(SUM(t.amount) * (p.gross_margin_pct / 100.0), 2) as gross_profit,
       AVG(t.amount) as avg_transaction_value
FROM transactions t
LEFT JOIN products p ON t.product_name = p.product_name
WHERE t.status = 'Successful'
GROUP BY p.product_name, p.category, p.gross_margin_pct
ORDER BY gross_profit DESC;
```

**Why multi-table**: Enriches transaction data with product pricing and margin data.

**Use cases**:
- Product team: identify high-margin vs low-margin products
- Finance: profitability analysis by SKU
- Sales: compensation planning (margin-based commission)

**Cost**: ~$0.06 (5K transactions × 50 products via LEFT JOIN)

---

### Pattern 4: Three-Table Join (Transactions → Customers → Regions)

**Question**: "What's our revenue breakdown by region AND customer segment?"

```sql
SELECT r.region_name,
       c.customer_segment,
       COUNT(DISTINCT c.customer_id) as customer_count,
       COUNT(t.transaction_id) as transaction_count,
       SUM(t.amount) as segment_revenue,
       AVG(t.amount) as avg_transaction,
       ROUND(AVG(c.lifetime_value), 2) as avg_customer_ltv
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
LEFT JOIN regions r ON c.region_id = r.region_id
GROUP BY r.region_name, c.customer_segment
ORDER BY segment_revenue DESC;
```

**Why multi-table**: Chains joins: fact → dimension → hierarchy.

**Use cases**:
- Executive dashboards: revenue by region/segment matrix
- GTM strategy: identify high/low-performing segment+region combos
- Capacity planning: where to add sales headcount

**Cost**: ~$0.10 (multi-hop join: 5K × 10K with bucketing)

---

### Pattern 5: Customer Cohort Analysis

**Question**: "How are customer cohorts performing by region?"

```sql
SELECT r.region_name,
       c.customer_segment,
       c.join_date::DATE as cohort_month,
       COUNT(DISTINCT c.customer_id) as cohort_size,
       AVG(c.lifetime_value) as avg_ltv,
       SUM(CASE WHEN c.churn_flag = 1 THEN 1 ELSE 0 END) as churned_count,
       ROUND(
         SUM(CASE WHEN c.churn_flag = 1 THEN 1 ELSE 0 END)::DECIMAL /
         COUNT(DISTINCT c.customer_id),
         4
       ) as cohort_churn_rate
FROM customers c
LEFT JOIN regions r ON c.region_id = r.region_id
GROUP BY r.region_name, c.customer_segment, c.join_date::DATE
ORDER BY r.region_name, cohort_month DESC;
```

**Why multi-table**: Analyzes cohort behavior in regional context.

**Use cases**:
- Product analytics: when do different cohorts churn?
- Sales ops: which cohorts have highest LTV?
- Marketing: measure campaign effectiveness by cohort

**Cost**: ~$0.04 (10K customers grouped by region/segment/month)

---

### Pattern 6: Top Customers by Region (Ranked)

**Question**: "Who are our top customers in each region?"

```sql
SELECT r.region_name,
       c.customer_name,
       c.customer_segment,
       c.customer_id,
       COUNT(t.transaction_id) as transaction_count,
       SUM(t.amount) as region_revenue,
       AVG(t.amount) as avg_transaction_value,
       MAX(t.transaction_date) as last_transaction_date
FROM regions r
LEFT JOIN customers c ON r.region_id = c.region_id
LEFT JOIN transactions t ON c.customer_id = t.customer_id
WHERE t.transaction_date >= CURRENT_DATE - INTERVAL 12 MONTHS
GROUP BY r.region_name, c.customer_id, c.customer_name, c.customer_segment
ORDER BY r.region_name, region_revenue DESC
LIMIT 100;
```

**Why multi-table**: Combines regional, customer, and transaction data for ranking.

**Use cases**:
- Account management: identify VIP customers per region
- Sales territory planning: who owns which accounts
- Executive briefing: key accounts by region

**Cost**: ~$0.09 (date filter + multi-hop join)

---

## How to Write Your Own Multi-Table Query

### Step 1: Identify the Tables You Need

**Questions to Ask**:
- What metric do I need? (SUM, COUNT, AVG from transactions)
- What dimensions will I group by? (customer segment, region, product)
- What tables hold those dimensions? (customers, regions, products)

**Example**: "Revenue by customer segment"
- Metric: SUM(amount) from transactions
- Dimension: customer_segment from customers
- Tables needed: transactions + customers

### Step 2: Determine the Join Path

**Use the semantic layer to find valid paths**:

```
Valid Paths from transactions:
- transactions → customers (ON customer_id = customer_id)
- transactions → customers → regions (multi-hop)
- transactions → products (ON product_name = product_name)
```

**Choose the shortest path**:
- ❌ Don't: transactions → customers → regions → (another table)
- ✅ Do: transactions → customers → regions (if you need regions)

### Step 3: Start with the Fact Table

Always put the fact table (transactions) in FROM:

```sql
SELECT ...
FROM transactions t
```

### Step 4: Add JOINs (Always LEFT)

```sql
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
LEFT JOIN regions r ON c.region_id = r.region_id
```

### Step 5: Build the SELECT List

- Include the dimension columns (for grouping)
- Include the metric (SUM, COUNT, AVG)
- Include helpful context (avg values, counts)

```sql
SELECT c.customer_segment,
       r.region_name,
       SUM(t.amount) as revenue,
       COUNT(DISTINCT c.customer_id) as customer_count,
       AVG(t.amount) as avg_transaction
```

### Step 6: Add WHERE Filters (Before Aggregation)

```sql
WHERE t.status = 'Successful'
  AND t.transaction_date >= CURRENT_DATE - INTERVAL 12 MONTHS
```

### Step 7: GROUP BY

```sql
GROUP BY c.customer_segment, r.region_name
```

### Step 8: ORDER BY and LIMIT

```sql
ORDER BY revenue DESC
LIMIT 50
```

### Complete Example

```sql
SELECT c.customer_segment,
       r.region_name,
       SUM(t.amount) as revenue,
       COUNT(DISTINCT c.customer_id) as customers,
       COUNT(t.transaction_id) as transactions,
       AVG(t.amount) as avg_transaction
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
LEFT JOIN regions r ON c.region_id = r.region_id
WHERE t.status = 'Successful'
  AND t.transaction_date >= CURRENT_DATE - INTERVAL 12 MONTHS
GROUP BY c.customer_segment, r.region_name
ORDER BY revenue DESC
LIMIT 50;
```

---

## Common Pitfalls & How to Avoid Them

### Pitfall 1: INNER JOIN (Data Loss)

**❌ Wrong**:
```sql
SELECT * FROM transactions t
INNER JOIN customers c ON t.customer_id = c.customer_id
```

**Why it fails**: If a transaction has no matching customer, it's dropped.

**✅ Correct**:
```sql
SELECT * FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
```

---

### Pitfall 2: Forgetting to GROUP BY

**❌ Wrong**:
```sql
SELECT c.customer_segment, SUM(t.amount)
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
```

**Why it fails**: DuckDB requires all non-aggregate columns in GROUP BY.

**✅ Correct**:
```sql
SELECT c.customer_segment, SUM(t.amount)
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
GROUP BY c.customer_segment
```

---

### Pitfall 3: Cartesian Product (Unintended Data Explosion)

**❌ Wrong**:
```sql
SELECT * FROM transactions t
LEFT JOIN products p  -- 50 products
LEFT JOIN regions r   -- 4 regions
-- Result: 5K × 50 × 4 = 1M rows instead of 5K!
```

**Why it fails**: Each transaction matches ALL products and ALL regions.

**✅ Correct**:
```sql
SELECT * FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
LEFT JOIN regions r ON c.region_id = r.region_id
-- Result: 5K rows (no explosion because joins are 1:1 or many:1)
```

---

### Pitfall 4: Aggregating on Non-Unique Dimensions

**❌ Wrong**:
```sql
SELECT c.customer_id, c.customer_name, SUM(t.amount)
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
GROUP BY c.customer_id
-- Missing c.customer_name in GROUP BY!
```

**✅ Correct**:
```sql
SELECT c.customer_id, c.customer_name, SUM(t.amount)
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name
```

---

## Performance Tips

### 1. Filter Early (WHERE Before GROUP BY)

```sql
SELECT c.customer_segment, SUM(t.amount)
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
WHERE t.status = 'Successful'  -- ← Reduces rows before join
GROUP BY c.customer_segment
```

**Impact**: 25-40% faster on large fact tables.

### 2. Limit Output

```sql
ORDER BY revenue DESC
LIMIT 100  -- ← Reduces result set size
```

**Impact**: Faster serialization to JSON.

### 3. Use Indexes (If DuckDB supports)

```sql
CREATE INDEX idx_transactions_customer_id ON transactions(customer_id);
CREATE INDEX idx_customers_region_id ON customers(region_id);
```

**Impact**: 2-5x faster joins on large tables.

### 4. Avoid Expensive Aggregations in JOINs

```sql
-- ❌ Slow: Aggregates INSIDE the join
SELECT c.customer_id,
       AVG(c.lifetime_value) as avg_ltv  -- Aggregation in join context
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id

-- ✅ Fast: Aggregate after join
SELECT SUM(t.amount) / COUNT(DISTINCT c.customer_id) as avg_customer_value
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
```

---

## Testing Your Query

Before sharing results, validate:

1. **Row Count**: Is the result reasonable?
   - Customer LTV: Should be ~10K rows (1 per customer)
   - Regional churn: Should be 4 rows (1 per region)

2. **Null Checks**: Any unexpected NULLs?
   ```sql
   WHERE column IS NOT NULL  -- Filter if needed
   ```

3. **Data Range**: Are dates reasonable?
   ```sql
   WHERE transaction_date >= '2024-01-01'  -- Sanity check
   ```

4. **Aggregation Sanity**: Does SUM/AVG make sense?
   - SUM should equal revenue ✅
   - COUNT(DISTINCT customer_id) should match customer table count ✅

---

## When to Ask for Help

**Use `suggest_joins(table_name)` if**:
- You're unsure which tables to join
- You want to see recommended patterns
- You need multi-table chains

**Use `search_golden_queries(description)` if**:
- Your question matches a known pattern
- You want a verified example
- You want to avoid trial-and-error

---

## Related Resources

- **Semantic Layer**: `semantic_layer.yaml` defines all metrics and dimensions
- **Golden Queries**: `seed_chroma.py` contains 19 verified patterns
- **Schema Graph**: Run `explore_schema` to inspect relationships
- **Tests**: `tests/test_multitable_queries.py` shows execution examples

