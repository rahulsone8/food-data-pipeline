# Pipeline Documentation — v5.1 (main) vs v6.0 (updates)

---

# 🔴 KEY DIFFERENCES: pipeline_main.py (v5.1) vs pipeline_main_updates.py (v6.0)

## Summary Table

| Aspect | v5.1 (pipeline_main.py) | v6.0 (pipeline_main_updates.py) |
|--------|-------------------------|--------------------------------|
| **Campaign file format** | `.xlsx` files | `.csv` files |
| **Campaign file pattern** | `cmp_*.xlsx` | `*_report.csv` |
| **STATUS_MAP** | 4 statuses | 6 statuses (handles "in progress", "canceled") |
| **WA_ACTION_ORDER[TOFU]** | `1` (same as VIEW_CATALOG) | `2` (separate stage) |
| **dim_restaurants** | No `seller_name` column | Adds `seller_name` = `restaurant_name` (alias) |
| **fact_orders.is_cancelled** | `bool` (True/False) | `int` (1/0) for MySQL SUM() |
| **fact_orders.has_coupon** | `bool` (True/False) | `int` (1/0) for MySQL SUM() |
| **agg_restaurant_daily** | No `coupon_orders` column | Adds `coupon_orders` column |
| **agg_customer_behavior** | No extra columns | Adds `cancellation_rate`, rounds `avg_order_value` |
| **Column stripping** | `df.columns = [str(col).strip() for col in df.columns]` | `df.columns = df.columns.str.strip()` |

---

## 📋 Detailed Differences

### 1️⃣ Campaign File Format Change

**v5.1 (pipeline_main.py):**
```python
campaign_files = glob.glob(f"{RAW}/cmp_*.xlsx")   # Excel files
df = pd.read_excel(filepath, engine="openpyxl")
```
- Pattern: `cmp_swh30_shawarmahouse.xlsx`
- Campaign name extracted: `cmp_swh30_shawarmahouse.xlsx` → `"swh30_shawarmahouse"`

**v6.0 (pipeline_main_updates.py):**
```python
campaign_files = glob.glob(f"{RAW}/*_report.csv")   # CSV files
df = pd.read_csv(filepath, low_memory=False)
```
- Patterns supported:
  - `mfc_swayowhatsapp_xlsx_-_report.csv` → `"mfc_swayowhatsapp"`
  - `just_fresh_point_march_-_report.csv` → `"just_fresh_point_march"`
  - `shawarma_house_diwali.csv` → `"shawarma_house_diwali"`

---

### 2️⃣ STATUS_MAP Extended

**v5.1:**
```python
STATUS_MAP = {
    "completed"  : "Completed",
    "accepted"   : "Accepted",
    "in-progress": "In-Progress",
    "cancelled"  : "Cancelled",
}
```

**v6.0:**
```python
STATUS_MAP = {
    "completed"  : "Completed",
    "accepted"   : "Accepted",
    "in-progress": "In-Progress",
    "in progress": "In-Progress",   # ← NEW: handles space variant
    "cancelled"  : "Cancelled",
    "canceled"   : "Cancelled",     # ← NEW: handles US spelling
}
```

---

### 3️⃣ WA Funnel Action Order

**v5.1:**
```python
WA_ACTION_ORDER = {"VIEW_CATALOG": 1, "TOFU": 1, ...}  # TOFU = 1 (same as VIEW_CATALOG)
```

**v6.0:**
```python
WA_ACTION_ORDER = {"VIEW_CATALOG": 1, "TOFU": 2, ...}  # TOFU = 2 (separate funnel stage)
```

---

### 4️⃣ dim_restaurants: seller_name alias

**v5.1:** No `seller_name` column

**v6.0:**
```python
if "seller_name" not in dim.columns:
    dim["seller_name"] = dim["restaurant_name"]   # alias for legacy joins
```

---

### 5️⃣ fact_orders: Boolean → Integer conversion

**v5.1:**
```python
orders["is_cancelled"] = orders["order_status"] == "Cancelled"      # bool
orders["has_coupon"]   = orders["coupon_value"].fillna(0) > 0       # bool
```

**v6.0:**
```python
orders["is_cancelled"] = (orders["order_status"] == "Cancelled").astype(int)  # 1/0
orders["has_coupon"]   = (orders["coupon_value"].fillna(0) > 0).astype(int)   # 1/0
```

**Why?** MySQL `SUM(is_cancelled)` works correctly with INT but may fail with Python bool.

---

### 6️⃣ agg_restaurant_daily: New column

**v5.1:** No `coupon_orders` column

**v6.0:**
```python
agg = fact_orders.groupby(...).agg(
    ...
    coupon_orders   = ("has_coupon", "sum"),   # ← NEW
)
```

---

### 7️⃣ agg_customer_behavior: New columns

**v5.1:** Basic metrics only

**v6.0:**
```python
agg["cancellation_rate"] = (
    agg["cancelled_orders"] / agg["total_orders"].replace(0, 1) * 100
).round(2)
agg["avg_order_value"] = agg["avg_order_value"].round(2)
```

---

## 🔄 Archive Pattern Difference

**v5.1:**
```python
f"{RAW}/cmp_*.xlsx"   # Archives Excel campaign files
```

**v6.0:**
```python
f"{RAW}/*_report.csv"   # Archives CSV campaign reports
```

---

# ═══════════════════════════════════════════════════════════════════════════

# Pipeline v5.1 — Complete Data Flow Documentation

## 📁 INPUT FILES (uploaded to `data/raw/`)

| File Pattern | Source | Contains |
|-------------|--------|----------|
| `data*.csv` | GrabFood + Swayo WhatsApp | Orders (item-level rows) |
| `response*.csv` | Swayo App | Orders (item-level rows) |
| `funnelanalysisswayo*.csv` | Swayo App | User journey events |
| `datewise_funnelanalysis_wa*.csv` | WhatsApp | User journey events |
| `cmp_*.xlsx` | Campaign reports | Message delivery status |

---

## 🔄 PIPELINE FLOW (13 MySQL Tables)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  RAW FILES                                                                   │
│  ─────────                                                                   │
│  data*.csv ─────────┬──────────────────────────────────────────────────────▶│
│  response*.csv ─────┤                                                        │
│                     ▼                                                        │
│              ┌─────────────────┐                                             │
│              │ restaurant_map  │  (internal lookup, not saved to MySQL)      │
│              └────────┬────────┘                                             │
│                       │                                                      │
│                       ▼                                                      │
│  ┌────────────────────┴────────────────────┐                                │
│  │         DIMENSION TABLES                 │                                │
│  │  ┌─────────────────┐ ┌────────────────┐ │                                │
│  │  │ dim_restaurants │ │ dim_customers  │ │                                │
│  │  └─────────────────┘ └────────────────┘ │                                │
│  └──────────────────────────────────────────┘                                │
│                       │                                                      │
│                       ▼                                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    FACT TABLES                                        │   │
│  │  ┌─────────────┐ ┌──────────────────┐ ┌────────────────┐              │   │
│  │  │ fact_orders │ │ fact_order_items │ │ fact_order_geo │              │   │
│  │  └─────────────┘ └──────────────────┘ └────────────────┘              │   │
│  │                                                                       │   │
│  │  ┌─────────────┐ ┌────────────────┐ ┌────────────────┐                │   │
│  │  │ fact_funnel │ │ fact_funnel_wa │ │ fact_campaigns │                │   │
│  │  │   (app)     │ │   (whatsapp)   │ │   (.xlsx)      │                │   │
│  │  └─────────────┘ └────────────────┘ └────────────────┘                │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                       │                                                      │
│                       ▼                                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                 AGGREGATION TABLES                                    │   │
│  │  ┌────────────────────┐ ┌───────────────────────┐                     │   │
│  │  │ agg_platform_daily │ │ agg_restaurant_daily  │                     │   │
│  │  └────────────────────┘ └───────────────────────┘                     │   │
│  │  ┌───────────────────────┐ ┌───────────────────────┐                  │   │
│  │  │ agg_funnel_conversion │ │ agg_customer_behavior │                  │   │
│  │  └───────────────────────┘ └───────────────────────┘                  │   │
│  │  ┌──────────────────────────┐                                         │   │
│  │  │ agg_campaign_performance │                                         │   │
│  │  └──────────────────────────┘                                         │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 DETAILED COLUMN MAPPINGS

### 1️⃣ `dim_restaurants` — One row per restaurant

| Source File | Source Column | → | Target Column | Cleaning Applied |
|-------------|---------------|---|---------------|------------------|
| data*.csv | `Provider_ID` | → | `shop_id` | `.astype(str).str.strip()` |
| data*.csv | `Seller_Name` | → | `restaurant_name` | `.str.strip()` |
| data*.csv | `Seller_City` | → | `city` | — |
| data*.csv | `Seller_Pincode` | → | `seller_pincode` | `pd.to_numeric(errors='coerce')` |
| response*.csv | `Shop ID` | → | `shop_id` | `.astype(str).str.strip()` (fills gaps) |
| response*.csv | `Restaurant Name` | → | `restaurant_name` | `.str.strip()` (fills gaps) |

**Cleaning:**
- Drop rows where `shop_id` OR `restaurant_name` is null
- Deduplicate on `shop_id` (keep first)
- data.csv is PRIMARY source (has city/pincode), response.csv fills missing shop_ids

**MySQL mode:** `REPLACE` (full refresh each run)

---

### 2️⃣ `dim_customers` — One row per unique customer

| Source File | Source Column | → | Target Column | Cleaning Applied |
|-------------|---------------|---|---------------|------------------|
| data*.csv | `customer_contact` | → | `customer_contact` | `clean_phone()` |
| data*.csv | `customer_name` | → | `customer_name` | — |
| data*.csv | `Pos_Order_ID` | → | `platform` | `detect_platform()` |
| response*.csv | `Customer Contact` | → | `customer_contact` | `clean_phone()` |
| response*.csv | `Customer Name` | → | `customer_name` | — |
| — | hardcoded | → | `platform` | `"swayo_app"` |

**`clean_phone()` function:**
```python
1. Handle NaN/None → return None
2. Convert float (9740688224.0) → string "9740688224"
3. Strip "+91" or "91" prefix
4. Remove all non-digits
5. Return last 10 digits if len >= 10, else None
```

**Cleaning:**
- Drop rows where `customer_contact` is None after cleaning
- Deduplicate on `customer_contact` (keep LAST occurrence)

**MySQL mode:** `REPLACE`

---

### 3️⃣ `fact_orders` — One row per ORDER (not per item!)

| Source File | Source Column | → | Target Column | Cleaning Applied |
|-------------|---------------|---|---------------|------------------|
| data*.csv | `Pos_Order_ID` | → | `order_id` | — |
| data*.csv | `Created` | → | `created_at` | `pd.to_datetime(format='mixed')` |
| data*.csv | `Total_Order_Value` | → | `order_value` | `pd.to_numeric(errors='coerce')` |
| data*.csv | `ONDC_Status` | → | `order_status` | `normalize_status()` |
| data*.csv | `customer_name` | → | `customer_name` | — |
| data*.csv | `customer_contact` | → | `customer_contact` | `clean_phone()` |
| data*.csv | `delivery_type` | → | `delivery_type` | — |
| data*.csv | `discount` | → | `discount` | `pd.to_numeric().fillna(0)` |
| data*.csv | `delivery_distance` | → | `delivery_distance` | — |
| data*.csv | `Buyer_NP_Name` | → | `channel` | — |
| data*.csv | `onboarded_date` | → | `shop_id` | `extract_shop_id_from_onboarded()` |
| data*.csv | `Provider_ID` | → | `shop_id` | fallback if onboarded_date fails |
| response*.csv | `Order ID` | → | `order_id` | — |
| response*.csv | `Created` | → | `created_at` | `pd.to_datetime(format='mixed')` |
| response*.csv | `Amount Paid by Customer` | → | `order_value` | `pd.to_numeric(errors='coerce')` |
| response*.csv | `Order Status` | → | `order_status` | `normalize_status()` |
| response*.csv | `Shop ID` | → | `shop_id` | — |
| response*.csv | `Coupon Value` | → | `coupon_value` | — |
| response*.csv | `Delivery Distance` | → | `delivery_distance` | — |
| response*.csv | `Total Packing Charge` | → | `packing_charge` | — |
| response*.csv | `Total Delivery Charge` | → | `delivery_charge` | — |
| response*.csv | `Total Convinience Charge` | → | `convenience_charge` | — |
| response*.csv | `Total Menu Discount` | → | `menu_discount` | — |
| response*.csv | `Total Cart Discount` | → | `cart_discount` | — |
| response*.csv | `Total Tax` | → | `tax` | — |

**Derived columns (computed):**
| Column | Formula |
|--------|---------|
| `platform` | `detect_platform(order_id)` → GFFW=gf_whatsapp, SWWA=swayo_whatsapp, SWYO=swayo_app |
| `restaurant_name` | `shop_id.map(restaurant_map)` |
| `order_date` | `created_at.dt.date` |
| `order_year` | `created_at.dt.year` |
| `order_month` | `created_at.dt.month` |
| `order_month_name` | `created_at.dt.strftime('%B')` |
| `order_week` | `created_at.dt.isocalendar().week` |
| `order_hour` | `created_at.dt.hour` |
| `order_dow` | `created_at.dt.day_name()` |
| `net_revenue` | `order_value - discount` |
| `is_cancelled` | `order_status == 'Cancelled'` |
| `has_coupon` | `coupon_value > 0` |

**`normalize_status()` mapping:**
```
completed   → Completed
accepted    → Accepted
in-progress → In-Progress
cancelled   → Cancelled
(other)     → as-is
(null)      → Unknown
```

**`detect_platform()` by order_id prefix:**
```
GFFW... → gf_whatsapp
SWWA... → swayo_whatsapp
SWYO... → swayo_app
(other) → unknown
```

**Cleaning:**
- Deduplicate data.csv on `order_id` first
- Remove response.csv rows where `order_id` already exists in data.csv
- Final dedup on `order_id`

**MySQL mode:** `APPEND`

---

### 4️⃣ `fact_order_items` — One row per ITEM

| Source File | Source Column | → | Target Column | Cleaning Applied |
|-------------|---------------|---|---------------|------------------|
| data*.csv | `Pos_Order_ID` | → | `order_id` | — |
| data*.csv | `Item_Name` | → | `product_name` | — |
| response*.csv | `Order ID` | → | `order_id` | — |
| response*.csv | `Item Names` | → | `product_name` | `.str.split(',')` then explode |

**Derived:**
| Column | Formula |
|--------|---------|
| `platform` | `detect_platform(order_id)` |

**Cleaning:**
- response.csv `Item Names` is comma-separated → explode to multiple rows
- Drop rows where `product_name` is null
- Drop rows where `product_name.strip().lower()` is `""`, `"nan"`, or `"none"`
- Remove response.csv rows where `order_id` already in data.csv

**MySQL mode:** `APPEND`

---

### 5️⃣ `fact_order_geo` — One row per ORDER with location data

| Source File | Source Column | → | Target Column | Cleaning Applied |
|-------------|---------------|---|---------------|------------------|
| data*.csv | `Pos_Order_ID` | → | `order_id` | — |
| data*.csv | `Delivery_Pincode` | → | `customer_pincode` | `extract_pincode_from_address()` (regex 6-digit) |
| data*.csv | `Delivery_Pincode` | → | `delivery_address` | raw full address string |
| data*.csv | `customer_contact` | → | `customer_contact` | `clean_phone()` |
| data*.csv | `delivery_distance` | → | `delivery_distance` | `pd.to_numeric(errors='coerce')` |
| response*.csv | `Order ID` | → | `order_id` | — |
| response*.csv | `Delivery Pincode` | → | `customer_pincode` | already numeric |
| response*.csv | `Customer Contact` | → | `customer_contact` | `clean_phone()` |
| response*.csv | `Delivery Distance` | → | `delivery_distance` | `pd.to_numeric(errors='coerce')` |

**Derived:**
| Column | Formula |
|--------|---------|
| `platform` | `detect_platform(order_id)` |
| `restaurant_name` | `shop_id.map(restaurant_map)` |
| `restaurant_pincode` | `shop_id.map(restaurant_map['seller_pincode'])` |
| `order_date` | `created_at.dt.date` |

**Note:** data.csv `Delivery_Pincode` contains FULL ADDRESS like:
`"204,MVR pg,..., Bengaluru, Karnataka 560100, India"`
→ regex extracts `560100` as customer_pincode

**MySQL mode:** `APPEND`

---

### 6️⃣ `fact_funnel` (Swayo App) — One row per funnel event

| Source File | Source Column | → | Target Column | Cleaning Applied |
|-------------|---------------|---|---------------|------------------|
| funnelanalysisswayo*.csv | `Action` | → | `action` | validate against allowed set |
| funnelanalysisswayo*.csv | `Timestamp` | → | `timestamp` | comma→dot, `pd.to_datetime(format='mixed')` |
| funnelanalysisswayo*.csv | `Customer Number` | → | `customer_contact` | `clean_phone()` |
| funnelanalysisswayo*.csv | `Shop ID` | → | `shop_id` | — |

**Valid actions:** `PDP`, `PLP`, `VIEW_CART`, `CHECKOUT`, `ORDER`

**Derived:**
| Column | Formula |
|--------|---------|
| `event_date` | `timestamp.dt.date` |
| `event_hour` | `timestamp.dt.hour` |
| `event_month` | `timestamp.dt.month` |
| `event_month_name` | `timestamp.dt.strftime('%B')` |
| `event_dow` | `timestamp.dt.day_name()` |
| `action_order` | `{PDP:1, PLP:2, VIEW_CART:3, CHECKOUT:4, ORDER:5}` |
| `funnel_source` | `"swayo_app"` |
| `row_id` | sequential index for checkpointing |

**Cleaning:**
- Timestamp fix: `"2026-02-26 01:33:19,979185"` → replace `,` with `.`
- Drop rows where `timestamp` is null after parsing
- Drop garbage PDP rows where `shop_id == "shop_id"` (header row leaked)
- Drop rows where `action` not in valid set
- **Incremental:** only load rows where `row_id > last_checkpoint`

**MySQL mode:** `APPEND`

---

### 7️⃣ `fact_funnel_wa` (WhatsApp) — One row per funnel event

| Source File | Source Column | → | Target Column | Cleaning Applied |
|-------------|---------------|---|---------------|------------------|
| datewise_funnelanalysis_wa*.csv | `timestamp` | → | `timestamp` | `pd.to_datetime(utc=True)` then strip tz |
| datewise_funnelanalysis_wa*.csv | `action` | → | `action` | validate against allowed set |
| datewise_funnelanalysis_wa*.csv | `campaign_id` | → | `campaign_id` | — |
| datewise_funnelanalysis_wa*.csv | `customer_code` | → | `customer_code` | (this is shop code, NOT customer!) |
| datewise_funnelanalysis_wa*.csv | `customer_name` | → | `customer_name` | — |
| datewise_funnelanalysis_wa*.csv | `customer_number` | → | `customer_contact` | `clean_phone()` |
| datewise_funnelanalysis_wa*.csv | `misc` | → | `wa_message` | greetings like "hi", "hello" |
| datewise_funnelanalysis_wa*.csv | `shop_id` | → | `shop_id` | — |
| datewise_funnelanalysis_wa*.csv | `shop_name` | → | `shop_name` | fallback for restaurant_name |

**Valid actions:** `VIEW_CATALOG`, `TOFU`, `VIEW_CART`, `CHECKOUT`, `ORDER`, `QUERY`

**Derived:**
| Column | Formula |
|--------|---------|
| `restaurant_name` | `shop_id.map(restaurant_map)` OR `shop_name` as fallback |
| `event_date` | `timestamp.dt.date` |
| `event_hour` | `timestamp.dt.hour` |
| `event_month` | `timestamp.dt.month` |
| `event_month_name` | `timestamp.dt.strftime('%B')` |
| `event_dow` | `timestamp.dt.day_name()` |
| `action_order` | `{VIEW_CATALOG:1, TOFU:1, VIEW_CART:3, CHECKOUT:4, ORDER:5, QUERY:6}` |
| `funnel_source` | `"whatsapp"` |
| `row_id` | sequential index for checkpointing |

**Cleaning:**
- Drop rows where `timestamp` is null
- Drop rows where `action` not in valid set
- **Incremental:** only load rows where `row_id > last_checkpoint`

**MySQL mode:** `APPEND`

---

### 8️⃣ `fact_campaigns` — One row per campaign recipient

| Source File | Source Column | → | Target Column | Cleaning Applied |
|-------------|---------------|---|---------------|------------------|
| cmp_*.xlsx | `mobile` | → | `mobile_number` | `clean_phone()` |
| cmp_*.xlsx | `sent_at` | → | `sent_at` | `pd.to_datetime(errors='coerce')` |
| cmp_*.xlsx | `delivered_at` | → | `delivered_at` | `pd.to_datetime(errors='coerce')` |
| cmp_*.xlsx | `read_at` | → | `read_at` | `pd.to_datetime(errors='coerce')` |
| cmp_*.xlsx | `scheduled_date` | → | `scheduled_date` | fix outliers < 2020 |
| cmp_*.xlsx | `scheduled_time` | → | `scheduled_time` | — |
| cmp_*.xlsx | `pitch_response` | → | `pitch_response` | — |
| — | filename | → | `campaign_name` | extracted from `cmp_XXX_name.xlsx` → `"XXX_name"` |
| — | hardcoded | → | `campaign_id` | `"cmp_{campaign_name}"` |

**Derived:**
| Column | Formula |
|--------|---------|
| `scheduled_at` | combine `scheduled_date` + `scheduled_time` |
| `is_sent` | `sent_at.notna()` → 1/0 |
| `is_delivered` | `delivered_at.notna()` → 1/0 |
| `is_read` | `read_at.notna()` → 1/0 |
| `delivery_status` | Read > Delivered > Sent > Failed |
| `loaded_at` | pipeline run timestamp |

**MySQL mode:** `APPEND`

---

## 📈 AGGREGATION TABLES

### 9️⃣ `agg_platform_daily`
Groups `fact_orders` by `(platform, order_date)`:
- `gmv`, `net_revenue`, `order_count`, `avg_order_value`
- `discount_given`, `coupon_orders`, `cancelled_count`, `cancellation_rate`

**MySQL mode:** `REPLACE`

### 🔟 `agg_restaurant_daily`
Groups `fact_orders` by `(restaurant_name, shop_id, platform, order_date)`:
- Same metrics as platform_daily

**MySQL mode:** `REPLACE`

### 1️⃣1️⃣ `agg_funnel_conversion`
Pivots `fact_funnel` (app only) by `(shop_id, event_date, action)`:
- `pdp_views`, `plp_views`, `cart_views`, `checkouts`, `orders_placed`
- Conversion rates: `plp_to_cart_rate`, `cart_to_checkout_rate`, etc.

**MySQL mode:** `REPLACE`

### 1️⃣2️⃣ `agg_customer_behavior`
Groups `fact_orders` by `customer_contact`:
- `total_orders`, `total_gmv`, `avg_order_value`, `total_discount`
- `first_order_date`, `last_order_date`, `platforms_used`
- `cancelled_orders`, `coupon_usage`
- `customer_segment`: VIP (≥10), Loyal (≥5), Repeat (≥2), One-time (1)

**MySQL mode:** `REPLACE`

### 1️⃣3️⃣ `agg_campaign_performance`
Groups `fact_campaigns` by `(campaign_name, campaign_id)`:
- `total_recipients`, `sent_count`, `delivered_count`, `read_count`
- `sent_rate_pct`, `delivered_rate_pct`, `read_rate_pct`
- `orders_after_24h`: count of recipients who placed ORDER in `fact_funnel_wa` within 24h of `read_at`
- `conversion_rate_pct`

**MySQL mode:** `REPLACE`

---

## 🧹 DATA CLEANING SUMMARY

| Function | What It Cleans |
|----------|----------------|
| `clean_phone()` | Normalizes to 10-digit Indian mobile, strips +91, returns None if invalid |
| `normalize_status()` | Maps order statuses to consistent capitalization |
| `detect_platform()` | Maps order_id prefix to platform label |
| `extract_shop_id_from_onboarded()` | Parses `{'shop_id': 'xxx'}` dict string |
| `extract_pincode_from_address()` | Regex extracts 6-digit pincode from address |

---

## ⏱️ INCREMENTAL LOADING (Checkpoints)

Funnel tables use checkpointing to avoid reloading old data:

```
data/processed/app_funnel_last_run.txt  → stores max(row_id) loaded
data/processed/wa_funnel_last_run.txt   → stores max(row_id) loaded
```

On each run:
1. Read checkpoint value
2. Only insert rows where `row_id > checkpoint`
3. Update checkpoint with new max

---

## 📦 AFTER PIPELINE RUNS

1. Raw files moved to `data/archive/`
2. Checkpoints updated
3. MySQL tables populated

**REPLACE tables:** Full refresh each run (dimensions, aggregations)
**APPEND tables:** Incremental (facts — may cause duplicates if same file re-uploaded!)
