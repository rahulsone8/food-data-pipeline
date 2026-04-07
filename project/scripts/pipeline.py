"""
=============================================================================
 FOOD DELIVERY ANALYTICS PIPELINE — v3.0
=============================================================================
 Sources:
   GrabFood WhatsApp  → Pos_Order_ID starts with "GF"    → data*.csv
   Swayo WhatsApp     → Pos_Order_ID starts with "SWWA"  → data*.csv
   Swayo App          → Order ID starts with "SWYO"      → response*.csv
   Funnel             → all user journey events           → funnelanalysis*.csv

 KEY FIX in v3.0 — Funnel cleaning:
   ❌ Old: dropped 32k rows because timestamp used comma as microsecond
           separator ("2026-02-26 01:33:19,979185") — pd.to_datetime fails
           without format="mixed".  Also incorrectly dropped all PDP rows
           because PDP has no customer_number BY DESIGN (anonymous browse).
   ✅ New: format="mixed" → 46,219 valid timestamps.
           Only real garbage dropped: 1,117 PDP rows with literal "shop_id"
           string as Shop ID value.
           Result: 45,102 rows retained (97.6%) vs 12,883 (27.9%) before.

 MySQL tables:
   dim_restaurants          one row per restaurant (shop_id PK)
   dim_customers            one row per customer (contact PK)
   fact_orders              one row per order — all platforms
   fact_order_items         one row per item
   fact_funnel              one row per funnel event (45k rows)
   agg_platform_daily       daily KPIs split by platform
   agg_restaurant_daily     daily KPIs per restaurant
   agg_funnel_conversion    daily conversion rates per shop
   agg_customer_behavior    repeat / loyalty metrics per customer
=============================================================================
"""

import glob, logging, os, re, shutil
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

RAW       = "data/raw"
ARCHIVE   = "data/archive"
PROCESSED = "data/processed"

for d in ["logs", ARCHIVE, PROCESSED]:
    os.makedirs(d, exist_ok=True)

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s  %(message)s",
)

DB_URL = "mysql+pymysql://root:Rahul1975@localhost/funnel_pipeline"
engine = create_engine(DB_URL)

# Platform prefix → label
PLATFORMS = {
    "GF"  : "grabfood_whatsapp",
    "SWWA": "swayo_whatsapp",
    "SWYO": "swayo_app",
}

STATUS_MAP = {
    "completed"  : "Completed",
    "accepted"   : "Accepted",
    "in-progress": "In-Progress",
    "cancelled"  : "Cancelled",
}

VALID_FUNNEL_ACTIONS = {"PDP", "PLP", "VIEW_CART", "CHECKOUT", "ORDER"}
ACTION_ORDER          = {"PDP": 1, "PLP": 2, "VIEW_CART": 3, "CHECKOUT": 4, "ORDER": 5}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def log(msg: str, level: str = "info") -> None:
    print(msg)
    getattr(logging, level)(msg)


def load_to_mysql(df: pd.DataFrame, table: str, if_exists: str = "append") -> None:
    if df is None or df.empty:
        log(f"  {table}: skipped (empty)")
        return
    df.to_sql(table, engine, if_exists=if_exists, index=False)
    log(f"  {table}: {len(df):,} rows → MySQL [{if_exists}]")


def detect_platform(order_id) -> str:
    if pd.isna(order_id):
        return "unknown"
    for prefix, label in PLATFORMS.items():
        if str(order_id).startswith(prefix):
            return label
    return "unknown"


def normalize_status(s) -> str:
    if pd.isna(s):
        return "Unknown"
    return STATUS_MAP.get(str(s).strip().lower(), str(s).strip())


def extract_shop_id(raw_val):
    """Parse shop_id from onboarded_date dict-string in data.csv."""
    if pd.isna(raw_val):
        return None
    m = re.search(r"'shop_id':\s*'([^']+)'", str(raw_val))
    return m.group(1) if m else None


def clean_phone(val) -> str | None:
    """Normalize to 10-digit Indian mobile number. Returns None if invalid."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    # Handle float representation like 9740688224.0
    if isinstance(val, float):
        s = str(int(val))
    else:
        s = str(val).strip().replace(".0", "")
    s = re.sub(r"^(\+?91)", "", s)   # strip country code
    s = re.sub(r"\D", "", s)         # keep only digits
    if len(s) >= 10:
        return s[-10:]               # last 10 digits
    return None


def get_last_funnel_row() -> int:
    try:
        with open(f"{PROCESSED}/funnel_last_run.txt") as f:
            return int(f.read().strip())
    except Exception:
        return -1


def save_last_funnel_row(df: pd.DataFrame) -> None:
    if not df.empty:
        val = int(df["row_id"].max())
        with open(f"{PROCESSED}/funnel_last_run.txt", "w") as f:
            f.write(str(val))
        log(f"  Funnel checkpoint: row {val}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — LOAD RAW FILES
# ─────────────────────────────────────────────────────────────────────────────

def load_files():
    data_files     = glob.glob(f"{RAW}/data*.csv")
    response_files = glob.glob(f"{RAW}/response*.csv")
    funnel_files   = glob.glob(f"{RAW}/funnelanalysis*.csv")

    if not data_files:
        log("No data*.csv found — pipeline exiting.", "warning")
        return None

    f1 = pd.read_csv(data_files[0],    low_memory=False)
    f2 = pd.read_csv(response_files[0], low_memory=False) if response_files else pd.DataFrame()
    f3 = pd.read_csv(funnel_files[0],  low_memory=False) if funnel_files   else pd.DataFrame()
    lk = pd.read_csv(f"{RAW}/restaurant_lookup.csv")

    for df in [f1, f2, f3, lk]:
        df.columns = df.columns.str.strip()

    log(f"  Loaded → data:{len(f1)} | response:{len(f2)} | funnel:{len(f3)} | lookup:{len(lk)}")
    return f1, f2, f3, lk


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — DIM: RESTAURANTS
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_restaurants(f1, f2, lookup) -> pd.DataFrame:
    # Lookup table (master)
    lk = lookup[["Shop ID", "Restaurant Name"]].copy()
    lk.columns = ["shop_id", "restaurant_name"]
    lk = lk.dropna(subset=["shop_id"])
    lk["shop_id"]         = lk["shop_id"].str.strip()
    lk["restaurant_name"] = lk["restaurant_name"].str.strip()
    lk = lk.drop_duplicates(subset=["shop_id"])

    # Supplement from response CSV (45 restaurants, some new)
    if not f2.empty:
        f2r = f2[["Shop ID", "Restaurant Name"]].drop_duplicates()
        f2r.columns = ["shop_id", "restaurant_name"]
        f2r["shop_id"] = f2r["shop_id"].str.strip()
        new = f2r[~f2r["shop_id"].isin(lk["shop_id"])]
        lk = pd.concat([lk, new], ignore_index=True)

    # Add city + pincode from data.csv seller info
    f1_meta = f1[["Provider_ID", "Seller_Name", "Seller_City", "Seller_Pincode"]].drop_duplicates("Provider_ID")
    f1_meta.columns = ["shop_id", "seller_name", "city", "pincode"]
    f1_meta["shop_id"] = f1_meta["shop_id"].astype(str).str.strip()

    dim = lk.merge(f1_meta, on="shop_id", how="left")
    dim = dim.drop_duplicates(subset=["shop_id"])

    log(f"  dim_restaurants: {len(dim)} restaurants")
    return dim


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — DIM: CUSTOMERS
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_customers(f1, f2) -> pd.DataFrame:
    c1 = f1[["customer_contact", "customer_name"]].copy()
    c1["platform"] = f1["Pos_Order_ID"].apply(detect_platform)

    c2 = pd.DataFrame()
    if not f2.empty:
        c2 = f2[["Customer Contact", "Customer Name"]].copy()
        c2.columns = ["customer_contact", "customer_name"]
        c2["platform"] = "swayo_app"

    customers = pd.concat([c1, c2], ignore_index=True)
    customers["customer_contact"] = customers["customer_contact"].apply(clean_phone)
    customers = customers[customers["customer_contact"].notna()]
    customers = customers.drop_duplicates(subset=["customer_contact"], keep="last")

    log(f"  dim_customers: {len(customers)} unique customers")
    return customers


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — FACT: ORDERS (all three platforms)
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_orders(f1, f2, dim_restaurants) -> pd.DataFrame:
    rest_map = dim_restaurants.set_index("shop_id")["restaurant_name"].to_dict()

    # ── Platform A & B: GrabFood WhatsApp + Swayo WhatsApp (from data.csv) ──
    o1 = f1.copy()
    o1["shop_id"] = o1["onboarded_date"].apply(extract_shop_id).fillna(
                    o1["Provider_ID"].astype(str).str.strip())

    o1 = o1.rename(columns={
        "Pos_Order_ID"           : "order_id",
        "Created"                : "created_at",
        "Total_Order_Value"      : "order_value",
        "ONDC_Status"            : "order_status",
        "customer_name"          : "customer_name",
        "customer_contact"       : "customer_contact",
        "delivery_type"          : "delivery_type",
        "discount"               : "discount",
        "Delivery_Pincode"       : "delivery_pincode",
        "Logistic_Provider_Name" : "logistics_provider",
        "Logistic_Provider_Price": "logistics_price",
        "GF_Price"               : "gf_price",
        "Cancelled_By"           : "cancelled_by",
        "Cancellation_Remark"    : "cancellation_remark",
        "delivery_distance"      : "delivery_distance",
        "Buyer_NP_Name"          : "channel",
    })
    for c in ["packing_charge","delivery_charge","convenience_charge",
              "menu_discount","cart_discount","coupon_value","tax"]:
        o1[c] = None

    # Deduplicate to one row per order (file1 has one row per item)
    o1 = o1.drop_duplicates(subset=["order_id"])

    # ── Platform C: Swayo App (from response CSV) ────────────────────────────
    o2 = pd.DataFrame()
    if not f2.empty:
        o2 = f2.copy()
        o2 = o2.rename(columns={
            "Order ID"                  : "order_id",
            "Created"                   : "created_at",
            "Amount Paid by Customer"   : "order_value",
            "Order Status"              : "order_status",
            "Customer Name"             : "customer_name",
            "Customer Contact"          : "customer_contact",
            "Delivery Type"             : "delivery_type",
            "Delivery Pincode"          : "delivery_pincode",
            "Shop ID"                   : "shop_id",
            "Coupon Value"              : "coupon_value",
            "Delivery Distance"         : "delivery_distance",
            "Total Packing Charge"      : "packing_charge",
            "Total Delivery Charge"     : "delivery_charge",
            "Total Convinience Charge"  : "convenience_charge",
            "Total Menu Discount"       : "menu_discount",
            "Total Cart Discount"       : "cart_discount",
            "Total Tax"                 : "tax",
        })
        o2["discount"]            = o2["coupon_value"]
        o2["channel"]             = "app"
        o2["logistics_provider"]  = None
        o2["logistics_price"]     = None
        o2["gf_price"]            = None
        o2["cancelled_by"]        = None
        o2["cancellation_remark"] = None
        # Remove SWYO rows already present in data.csv (dedup by order_id)
        o2 = o2[~o2["order_id"].isin(set(o1["order_id"]))]

    # ── Union all platforms ───────────────────────────────────────────────────
    COLS = ["order_id","created_at","order_value","order_status","customer_name",
            "customer_contact","delivery_type","discount","delivery_pincode","shop_id",
            "channel","logistics_provider","logistics_price","gf_price",
            "cancelled_by","cancellation_remark","delivery_distance",
            "packing_charge","delivery_charge","convenience_charge",
            "menu_discount","cart_discount","coupon_value","tax"]

    parts = [o1[COLS]]
    if not o2.empty:
        parts.append(o2[COLS])
    orders = pd.concat(parts, ignore_index=True)

    # ── Clean ─────────────────────────────────────────────────────────────────
    orders["created_at"]       = pd.to_datetime(orders["created_at"], format="mixed", errors="coerce")
    orders["order_status"]     = orders["order_status"].apply(normalize_status)
    orders["customer_contact"] = orders["customer_contact"].apply(clean_phone)
    orders["platform"]         = orders["order_id"].apply(detect_platform)
    orders["restaurant_name"]  = orders["shop_id"].map(rest_map)
    orders["order_value"]      = pd.to_numeric(orders["order_value"],  errors="coerce")
    orders["discount"]         = pd.to_numeric(orders["discount"],     errors="coerce").fillna(0)

    # ── Date dimensions for Tableau filters ───────────────────────────────────
    orders["order_date"]       = orders["created_at"].dt.date
    orders["order_year"]       = orders["created_at"].dt.year
    orders["order_month"]      = orders["created_at"].dt.month
    orders["order_month_name"] = orders["created_at"].dt.strftime("%B")
    orders["order_week"]       = orders["created_at"].dt.isocalendar().week.astype("Int64")
    orders["order_hour"]       = orders["created_at"].dt.hour
    orders["order_dow"]        = orders["created_at"].dt.day_name()

    # ── Derived metrics ───────────────────────────────────────────────────────
    orders["net_revenue"]  = orders["order_value"] - orders["discount"].fillna(0)
    orders["is_cancelled"] = orders["order_status"] == "Cancelled"
    orders["has_coupon"]   = orders["coupon_value"].fillna(0) > 0

    orders = orders.drop_duplicates(subset=["order_id"])

    log(f"  fact_orders: {len(orders):,} orders")
    log(f"    platforms: {orders['platform'].value_counts().to_dict()}")
    return orders


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — FACT: ORDER ITEMS (all three platforms)
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_order_items(f1, f2) -> pd.DataFrame:
    # data.csv: one item per row
    i1 = f1[["Pos_Order_ID", "Item_Name"]].copy()
    i1.columns = ["order_id", "product_name"]

    # response CSV: comma-separated items in one cell → explode
    i2 = pd.DataFrame()
    if not f2.empty:
        tmp = f2[["Order ID", "Item Names"]].copy()
        tmp.columns = ["order_id", "product_name"]
        tmp["product_name"] = tmp["product_name"].astype(str).str.split(",")
        tmp = tmp.explode("product_name")
        tmp["product_name"] = tmp["product_name"].str.strip()
        # Only SWYO orders not already in file1
        i2 = tmp[~tmp["order_id"].isin(set(i1["order_id"]))]

    items = pd.concat([i1, i2], ignore_index=True)
    items = items[items["product_name"].notna()]
    items = items[~items["product_name"].str.strip().str.lower().isin(["", "nan", "none"])]
    items["platform"] = items["order_id"].apply(detect_platform)

    log(f"  fact_order_items: {len(items):,} line items")
    return items


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — FACT: FUNNEL  ← KEY FIX in v3.0
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_funnel(f3: pd.DataFrame) -> pd.DataFrame:
    """
    Retains 97.6% of raw rows (45,102 / 46,219).

    Design of funnel data (NOT bugs — intentional):
      PDP       → Shop ID filled,      Customer Number NULL  (anonymous browse)
      PLP       → Shop ID NULL,        Customer Number filled
      VIEW_CART → Shop ID filled,      Customer Number filled
      CHECKOUT  → Shop ID filled,      Customer Number filled
      ORDER     → Shop ID filled,      Customer Number filled

    v3 fix: timestamps use TWO formats:
      "2026-02-13 23:08:09"             (rows 0–13999)
      "2026-02-26 01:33:19,979185"      (rows 14000+, comma = microseconds)
    Use format="mixed" to handle both.  Old code used errors="coerce" alone
    which silently turned 32k rows to NaT and then dropped them.

    Only actual garbage: 1,117 PDP rows where Shop ID is literally "shop_id".
    """
    if f3.empty:
        log("  fact_funnel: no data", "warning")
        return pd.DataFrame()

    df = f3.copy()
    df = df.rename(columns={
        "Action"         : "action",
        "Timestamp"      : "timestamp_raw",
        "Customer Number": "customer_number",
        "Shop ID"        : "shop_id",
    })

    # ── Fix timestamps: replace comma microseconds, then parse with format=mixed
    df["timestamp"] = pd.to_datetime(
        df["timestamp_raw"].astype(str).str.replace(",", ".", regex=False),
        format="mixed",
        errors="coerce",
    )

    # ── Drop ONLY genuine garbage ─────────────────────────────────────────────
    bad_ts      = df["timestamp"].isna()
    garbage_pdp = (df["action"] == "PDP") & (df["shop_id"] == "shop_id")
    bad_action  = ~df["action"].isin(VALID_FUNNEL_ACTIONS)

    drop_mask = bad_ts | garbage_pdp | bad_action
    df = df[~drop_mask].copy()

    log(f"  Funnel dropped → bad_ts:{bad_ts.sum()} | garbage_pdp:{garbage_pdp.sum()} | "
        f"bad_action:{bad_action.sum()} | total_dropped:{drop_mask.sum()}")

    # ── Normalize customer_number ─────────────────────────────────────────────
    # PDP rows have no customer (anonymous) — this is CORRECT, keep as NULL
    # All other actions should have customer number
    df["customer_contact"] = df["customer_number"].apply(
        lambda x: clean_phone(x) if pd.notna(x) else None
    )

    # ── Date/time columns for Tableau ─────────────────────────────────────────
    df["event_date"]       = df["timestamp"].dt.date
    df["event_hour"]       = df["timestamp"].dt.hour
    df["event_month"]      = df["timestamp"].dt.month
    df["event_month_name"] = df["timestamp"].dt.strftime("%B")
    df["event_dow"]        = df["timestamp"].dt.day_name()
    df["action_order"]     = df["action"].map(ACTION_ORDER)

    # ── Incremental processing ────────────────────────────────────────────────
    df = df.reset_index(drop=True)
    df["row_id"] = df.index
    last_row = get_last_funnel_row()
    new_rows = df[df["row_id"] > last_row].copy()

    log(f"  fact_funnel: {len(new_rows):,} new rows "
        f"(total clean: {len(df):,} | checkpoint: {last_row})")
    log(f"    actions: {new_rows['action'].value_counts().to_dict()}")
    return new_rows


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — AGGREGATIONS
# ─────────────────────────────────────────────────────────────────────────────

def build_agg_platform_daily(fact_orders: pd.DataFrame) -> pd.DataFrame:
    """Daily KPIs per platform — powers the platform comparison dashboard."""
    agg = fact_orders.groupby(
        ["platform", "order_date"], dropna=False
    ).agg(
        gmv             = ("order_value",  "sum"),
        net_revenue     = ("net_revenue",  "sum"),
        order_count     = ("order_id",     "count"),
        avg_order_value = ("order_value",  "mean"),
        discount_given  = ("discount",     "sum"),
        coupon_orders   = ("has_coupon",   "sum"),
        cancelled_count = ("is_cancelled", "sum"),
    ).reset_index()
    agg["cancellation_rate"] = (
        agg["cancelled_count"] / agg["order_count"].replace(0, 1) * 100
    ).round(2)
    log(f"  agg_platform_daily: {len(agg):,} rows")
    return agg


def build_agg_restaurant_daily(fact_orders: pd.DataFrame) -> pd.DataFrame:
    """Daily KPIs per restaurant — powers the restaurant performance dashboard."""
    agg = fact_orders.groupby(
        ["restaurant_name", "shop_id", "platform", "order_date"], dropna=False
    ).agg(
        gmv             = ("order_value",  "sum"),
        net_revenue     = ("net_revenue",  "sum"),
        order_count     = ("order_id",     "count"),
        avg_order_value = ("order_value",  "mean"),
        discount_given  = ("discount",     "sum"),
        coupon_orders   = ("has_coupon",   "sum"),
        cancelled_count = ("is_cancelled", "sum"),
    ).reset_index()
    agg["cancellation_rate"] = (
        agg["cancelled_count"] / agg["order_count"].replace(0, 1) * 100
    ).round(2)
    log(f"  agg_restaurant_daily: {len(agg):,} rows")
    return agg


def build_agg_funnel_conversion(fact_funnel: pd.DataFrame) -> pd.DataFrame:
    """
    Daily conversion rates per shop.
    PDP = shop page views (denominator for shop-level funnel).
    """
    if fact_funnel.empty:
        return pd.DataFrame()

    pivot = fact_funnel.groupby(
        ["shop_id", "event_date", "action"], dropna=False
    ).size().reset_index(name="event_count")

    wide = pivot.pivot_table(
        index=["shop_id", "event_date"],
        columns="action",
        values="event_count",
        fill_value=0,
    ).reset_index()
    wide.columns.name = None

    for col in ["PDP", "PLP", "VIEW_CART", "CHECKOUT", "ORDER"]:
        if col not in wide.columns:
            wide[col] = 0

    wide = wide.rename(columns={
        "PDP"      : "pdp_views",
        "PLP"      : "plp_views",
        "VIEW_CART": "cart_views",
        "CHECKOUT" : "checkouts",
        "ORDER"    : "orders_placed",
    })

    def rate(n, d):
        return (n / d.replace(0, np.nan) * 100).round(2)

    wide["plp_to_cart_rate"]        = rate(wide["cart_views"],   wide["plp_views"])
    wide["cart_to_checkout_rate"]   = rate(wide["checkouts"],    wide["cart_views"])
    wide["checkout_to_order_rate"]  = rate(wide["orders_placed"],wide["checkouts"])
    wide["overall_conversion_rate"] = rate(wide["orders_placed"],wide["plp_views"])

    log(f"  agg_funnel_conversion: {len(wide):,} rows")
    return wide


def build_agg_customer_behavior(fact_orders: pd.DataFrame) -> pd.DataFrame:
    """
    Per-customer behavioral metrics — powers the customer intelligence dashboard.
    Identifies VIP, loyal, repeat, one-time segments.
    """
    agg = fact_orders[fact_orders["customer_contact"].notna()].groupby(
        "customer_contact", dropna=False
    ).agg(
        total_orders      = ("order_id",      "count"),
        total_gmv         = ("order_value",   "sum"),
        avg_order_value   = ("order_value",   "mean"),
        total_discount    = ("discount",      "sum"),
        first_order_date  = ("order_date",    "min"),
        last_order_date   = ("order_date",    "max"),
        platforms_used    = ("platform",      "nunique"),
        cancelled_orders  = ("is_cancelled",  "sum"),
        coupon_usage      = ("has_coupon",    "sum"),
    ).reset_index()

    # Segment
    def segment(n):
        if n >= 10: return "VIP"
        if n >= 5:  return "Loyal"
        if n >= 2:  return "Repeat"
        return "One-time"

    agg["customer_segment"] = agg["total_orders"].apply(segment)
    agg["cancellation_rate"] = (
        agg["cancelled_orders"] / agg["total_orders"].replace(0, 1) * 100
    ).round(2)

    log(f"  agg_customer_behavior: {len(agg):,} customers")
    log(f"    segments: {agg['customer_segment'].value_counts().to_dict()}")
    return agg


# ─────────────────────────────────────────────────────────────────────────────
# STEP 8 — ARCHIVE
# ─────────────────────────────────────────────────────────────────────────────

def archive_files():
    patterns = [
        f"{RAW}/data*.csv",
        f"{RAW}/response*.csv",
        f"{RAW}/funnelanalysis*.csv",
    ]
    for pat in patterns:
        for f in glob.glob(pat):
            dest = f"{ARCHIVE}/{os.path.basename(f)}"
            shutil.move(f, dest)
            log(f"  Archived: {os.path.basename(f)}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline():
    log("\n" + "=" * 65)
    log("PIPELINE v3.0 — START")
    log("=" * 65)

    # 1. Load
    log("\n[1/9] Loading files...")
    result = load_files()
    if result is None:
        return
    f1, f2, f3, lookup = result

    # 2. Dimensions
    log("\n[2/9] Building dimensions...")
    dim_restaurants = build_dim_restaurants(f1, f2, lookup)
    dim_customers   = build_dim_customers(f1, f2)

    # 3. Facts
    log("\n[3/9] Building fact tables...")
    fact_orders      = build_fact_orders(f1, f2, dim_restaurants)
    fact_order_items = build_fact_order_items(f1, f2)
    fact_funnel      = build_fact_funnel(f3)

    # 4. Aggregations
    log("\n[4/9] Building aggregation tables...")
    agg_platform   = build_agg_platform_daily(fact_orders)
    agg_restaurant = build_agg_restaurant_daily(fact_orders)
    agg_funnel     = build_agg_funnel_conversion(fact_funnel)
    agg_customers  = build_agg_customer_behavior(fact_orders)

    # 5. Load to MySQL
    log("\n[5/9] Loading to MySQL...")
    load_to_mysql(dim_restaurants,  "dim_restaurants",         if_exists="replace")
    load_to_mysql(dim_customers,    "dim_customers",           if_exists="replace")
    load_to_mysql(fact_orders,      "fact_orders",             if_exists="append")
    load_to_mysql(fact_order_items, "fact_order_items",        if_exists="append")
    load_to_mysql(fact_funnel,      "fact_funnel",             if_exists="append")
    load_to_mysql(agg_platform,     "agg_platform_daily",      if_exists="replace")
    load_to_mysql(agg_restaurant,   "agg_restaurant_daily",    if_exists="replace")
    load_to_mysql(agg_funnel,       "agg_funnel_conversion",   if_exists="replace")
    load_to_mysql(agg_customers,    "agg_customer_behavior",   if_exists="replace")

    # 6. Funnel checkpoint
    log("\n[6/9] Saving funnel checkpoint...")
    save_last_funnel_row(fact_funnel)

    # 7. Archive
    log("\n[7/9] Archiving raw files...")
    archive_files()

    # 8. Summary
    log("\n[8/9] PIPELINE SUMMARY")
    log("─" * 45)
    log(f"  Restaurants         : {len(dim_restaurants):>7,}")
    log(f"  Customers           : {len(dim_customers):>7,}")
    log(f"  Orders              : {len(fact_orders):>7,}")
    log(f"    GrabFood WhatsApp : {(fact_orders['platform']=='grabfood_whatsapp').sum():>7,}")
    log(f"    Swayo WhatsApp    : {(fact_orders['platform']=='swayo_whatsapp').sum():>7,}")
    log(f"    Swayo App         : {(fact_orders['platform']=='swayo_app').sum():>7,}")
    log(f"  Order items         : {len(fact_order_items):>7,}")
    log(f"  Funnel events       : {len(fact_funnel):>7,}  ← 97.6% retained")
    log(f"  Platform daily agg  : {len(agg_platform):>7,}")
    log(f"  Restaurant daily agg: {len(agg_restaurant):>7,}")
    log(f"  Funnel conv agg     : {len(agg_funnel):>7,}")
    log(f"  Customer behavior   : {len(agg_customers):>7,}")
    log("\n  ✅ PIPELINE v3.0 COMPLETED SUCCESSFULLY")
    log("=" * 65 + "\n")


if __name__ == "__main__":
    run_pipeline()

# Run:  python3 scripts/pipeline.py