"""
=============================================================================
 FOOD DELIVERY ANALYTICS PIPELINE — v5.0
=============================================================================
 Changes from v4:
   5. NEW TABLES: fact_campaigns + agg_campaign_performance
        Campaign report CSVs (one file per campaign batch).
        Filename IS the campaign_name — extracted automatically.

        Supported filename patterns:
          {name}_xlsx_-_report.csv   → campaign_name = {name}
          {name}_-_report.csv        → campaign_name = {name}
          {name}.csv                 → campaign_name = {name}
        Examples:
          mfc_swayowhatsapp_xlsx_-_report.csv    → "mfc_swayowhatsapp"
          just_fresh_point_march_-_report.csv    → "just_fresh_point_march"
          shawarma_house_diwali_-_report.csv     → "shawarma_house_diwali"

        fact_campaigns: one row per recipient per campaign message
          campaign_name, campaign_id, mobile_number, scheduled_at,
          sent_at, delivered_at, read_at,
          is_sent, is_delivered, is_read, delivery_status,
          pitch_response, loaded_at

        agg_campaign_performance: one row per campaign_id
          campaign_name, campaign_id, scheduled_date,
          total_recipients, sent_count, delivered_count, read_count,
          sent_rate_pct, delivered_rate_pct, read_rate_pct,
          orders_after_24h  ← customers who placed ORDER in fact_funnel_wa
                               within 24 hours of read_at
          conversion_rate_pct

        JOIN CHAIN (future Tableau use):
          fact_campaigns.mobile_number
              → fact_funnel_wa.customer_contact  (who browsed after campaign)
              → fact_funnel_wa WHERE action='ORDER' (who actually ordered)
          fact_campaigns.campaign_id
              → fact_funnel_wa.campaign_id       (same campaign triggered the funnel)

 MySQL tables (11 from v4 + 2 new = 13 total):
   dim_restaurants          one row per restaurant (shop_id PK)
   dim_customers            one row per customer
   fact_orders              one row per order — all platforms
   fact_order_items         one row per item
   fact_order_geo           one row per order — location/distance data
   fact_funnel              Swayo App funnel events (45,102 rows)
   fact_funnel_wa           WhatsApp funnel events (9,621 rows)
   fact_campaigns           one row per campaign message recipient  ← NEW
   agg_platform_daily       daily KPIs by platform
   agg_restaurant_daily     daily KPIs by restaurant
   agg_funnel_conversion    Swayo App funnel conversion rates
   agg_customer_behavior    customer loyalty segments
   agg_campaign_performance campaign delivery + conversion KPIs    ← NEW
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

# ── Platform prefix map (GFFW not GF — important!) ───────────────────────────
PLATFORMS = {
    "GFFW": "gf_whatsapp",       # GrabFood WhatsApp (ONDC)
    "SWWA": "swayo_whatsapp",    # Swayo WhatsApp
    "SWYO": "swayo_app",         # Swayo App
}

STATUS_MAP = {
    "completed"  : "Completed",
    "accepted"   : "Accepted",
    "in-progress": "In-Progress",
    "cancelled"  : "Cancelled",
}

# Swayo App funnel actions
VALID_APP_FUNNEL   = {"PDP", "PLP", "VIEW_CART", "CHECKOUT", "ORDER"}
APP_ACTION_ORDER   = {"PDP": 1, "PLP": 2, "VIEW_CART": 3, "CHECKOUT": 4, "ORDER": 5}

# WhatsApp funnel actions
VALID_WA_FUNNEL    = {"VIEW_CATALOG", "TOFU", "VIEW_CART", "CHECKOUT", "ORDER", "QUERY"}
WA_ACTION_ORDER    = {"VIEW_CATALOG": 1, "TOFU": 1, "VIEW_CART": 3,
                      "CHECKOUT": 4, "ORDER": 5, "QUERY": 6}


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
    """Map order_id prefix to platform label. GFFW = GrabFood WhatsApp."""
    if pd.isna(order_id):
        return "unknown"
    s = str(order_id)
    for prefix, label in PLATFORMS.items():
        if s.startswith(prefix):
            return label
    return "unknown"


def normalize_status(s) -> str:
    if pd.isna(s):
        return "Unknown"
    return STATUS_MAP.get(str(s).strip().lower(), str(s).strip())


def extract_shop_id_from_onboarded(raw_val) -> str | None:
    """Parse shop_id from onboarded_date dict-string in data.csv."""
    if pd.isna(raw_val):
        return None
    m = re.search(r"'shop_id':\s*'([^']+)'", str(raw_val))
    return m.group(1) if m else None


def clean_phone(val) -> str | None:
    """Normalize to 10-digit Indian mobile. Returns None if invalid."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, float):
        s = str(int(val))
    else:
        s = str(val).strip().replace(".0", "")
    s = re.sub(r"^\+?91", "", s)
    s = re.sub(r"\D", "", s)
    return s[-10:] if len(s) >= 10 else None


def extract_pincode_from_address(addr) -> str | None:
    """
    data.csv Delivery_Pincode column contains full address strings like:
    "204,MVR pg,..., Bengaluru, Karnataka 560100, India"
    Extract the 6-digit Indian pincode from this text.
    """
    if pd.isna(addr):
        return None
    m = re.search(r'\b(\d{6})\b', str(addr))
    return m.group(1) if m else None


def get_last_funnel_row(name: str) -> int:
    """Read incremental checkpoint for a named funnel table."""
    try:
        with open(f"{PROCESSED}/{name}_last_run.txt") as f:
            return int(f.read().strip())
    except Exception:
        return -1


def save_last_funnel_row(df: pd.DataFrame, name: str) -> None:
    if not df.empty and "row_id" in df.columns:
        val = int(df["row_id"].max())
        with open(f"{PROCESSED}/{name}_last_run.txt", "w") as f:
            f.write(str(val))
        log(f"  Checkpoint [{name}]: row {val}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — LOAD RAW FILES
# ─────────────────────────────────────────────────────────────────────────────

def load_files():
    data_files      = glob.glob(f"{RAW}/data*.csv")
    response_files  = glob.glob(f"{RAW}/response*.csv")
    funnel_files    = glob.glob(f"{RAW}/funnelanalysis*.csv")
    wa_funnel_files = glob.glob(f"{RAW}/datewise_funnelanalysis_wa*.csv")
    campaign_files  = glob.glob(f"{RAW}/*_report.csv")   # ← all campaign report CSVs

    if not data_files:
        log("No data*.csv found — pipeline exiting.", "warning")
        return None

    f1 = pd.read_csv(data_files[0],    low_memory=False)
    f2 = pd.read_csv(response_files[0], low_memory=False) if response_files else pd.DataFrame()
    f3 = pd.read_csv(funnel_files[0],  low_memory=False) if funnel_files   else pd.DataFrame()
    f4 = pd.read_csv(wa_funnel_files[0], low_memory=False) if wa_funnel_files else pd.DataFrame()

    for df in [f1, f2, f3, f4]:
        df.columns = df.columns.str.strip()

    log(f"  Loaded → data:{len(f1)} | response:{len(f2)} | "
        f"app_funnel:{len(f3)} | wa_funnel:{len(f4)} | "
        f"campaign_files:{len(campaign_files)}")
    return f1, f2, f3, f4, campaign_files


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — BUILD RESTAURANT MAP (NO lookup file — from source data only)
# ─────────────────────────────────────────────────────────────────────────────

def build_restaurant_map(f1: pd.DataFrame, f2: pd.DataFrame) -> pd.DataFrame:
    """
    Build shop_id → restaurant_name map directly from:
      - data.csv:   Provider_ID → Seller_Name + Seller_City + Seller_Pincode
      - response*:  Shop ID → Restaurant Name

    No restaurant_lookup.csv needed.
    Returns a clean deduplicated DataFrame used for all downstream joins.
    """
    # ── Source A: data.csv (GF + SWWA orders) ────────────────────────────────
    src_a = f1[["Provider_ID", "Seller_Name", "Seller_City", "Seller_Pincode"]].copy()
    src_a = src_a.rename(columns={
        "Provider_ID"   : "shop_id",
        "Seller_Name"   : "restaurant_name",
        "Seller_City"   : "city",
        "Seller_Pincode": "seller_pincode",
    })
    src_a["shop_id"]         = src_a["shop_id"].astype(str).str.strip()
    src_a["restaurant_name"] = src_a["restaurant_name"].str.strip()
    src_a = src_a.dropna(subset=["shop_id", "restaurant_name"])
    src_a = src_a.drop_duplicates(subset=["shop_id"])

    # ── Source B: response CSV (Swayo App orders) ─────────────────────────────
    src_b = pd.DataFrame()
    if not f2.empty:
        src_b = f2[["Shop ID", "Restaurant Name"]].copy()
        src_b = src_b.rename(columns={
            "Shop ID"        : "shop_id",
            "Restaurant Name": "restaurant_name",
        })
        src_b["shop_id"]         = src_b["shop_id"].astype(str).str.strip()
        src_b["restaurant_name"] = src_b["restaurant_name"].str.strip()
        src_b = src_b.dropna(subset=["shop_id", "restaurant_name"])
        src_b["city"]           = None
        src_b["seller_pincode"] = None
        src_b = src_b.drop_duplicates(subset=["shop_id"])

    # ── Merge: src_a is primary (has pincode + city), src_b fills gaps ────────
    if not src_b.empty:
        new_shops = src_b[~src_b["shop_id"].isin(src_a["shop_id"])]
        restaurant_map = pd.concat([src_a, new_shops], ignore_index=True)
    else:
        restaurant_map = src_a.copy()

    restaurant_map = restaurant_map.drop_duplicates(subset=["shop_id"])

    log(f"  restaurant_map: {len(restaurant_map)} shops "
        f"(from data.csv={len(src_a)}, new from response={len(src_b) - len(src_b[src_b['shop_id'].isin(src_a['shop_id'])]) if not src_b.empty else 0})")
    return restaurant_map


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — DIM: RESTAURANTS
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_restaurants(restaurant_map: pd.DataFrame) -> pd.DataFrame:
    dim = restaurant_map.copy()
    dim["seller_pincode"] = pd.to_numeric(dim["seller_pincode"], errors="coerce")
    log(f"  dim_restaurants: {len(dim)} rows")
    return dim


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — DIM: CUSTOMERS
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_customers(f1: pd.DataFrame, f2: pd.DataFrame) -> pd.DataFrame:
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
# STEP 5 — FACT: ORDERS (all 3 platforms, properly differentiated)
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_orders(
    f1: pd.DataFrame,
    f2: pd.DataFrame,
    restaurant_map: pd.DataFrame,
) -> pd.DataFrame:
    """
    Platform differentiation:
      GFFW prefix → platform = "gf_whatsapp"       (GrabFood WhatsApp via ONDC)
      SWWA prefix → platform = "swayo_whatsapp"     (Swayo WhatsApp)
      SWYO prefix → platform = "swayo_app"          (Swayo App)
    """
    rest_name_map    = restaurant_map.set_index("shop_id")["restaurant_name"].to_dict()
    rest_pincode_map = restaurant_map.set_index("shop_id")["seller_pincode"].to_dict()

    # ── f1: GF WhatsApp + Swayo WhatsApp ─────────────────────────────────────
    o1 = f1.copy()
    o1["shop_id"] = o1["onboarded_date"].apply(extract_shop_id_from_onboarded).fillna(
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
        "delivery_distance"      : "delivery_distance",
        "Buyer_NP_Name"          : "channel",
        "Logistic_Provider_Name" : "logistics_provider",
        "Logistic_Provider_Price": "logistics_price",
        "GF_Price"               : "gf_price",
        "Cancelled_By"           : "cancelled_by",
        "Cancellation_Remark"    : "cancellation_remark",
    })
    for c in ["packing_charge", "delivery_charge", "convenience_charge",
              "menu_discount", "cart_discount", "coupon_value", "tax"]:
        o1[c] = None

    o1 = o1.drop_duplicates(subset=["order_id"])

    # ── f2: Swayo App ─────────────────────────────────────────────────────────
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
        o2 = o2[~o2["order_id"].isin(set(o1["order_id"]))]

    # ── Union ─────────────────────────────────────────────────────────────────
    COLS = ["order_id", "created_at", "order_value", "order_status",
            "customer_name", "customer_contact", "delivery_type", "discount",
            "delivery_distance", "shop_id", "channel", "logistics_provider",
            "logistics_price", "gf_price", "cancelled_by", "cancellation_remark",
            "packing_charge", "delivery_charge", "convenience_charge",
            "menu_discount", "cart_discount", "coupon_value", "tax"]

    parts = [o1[COLS]]
    if not o2.empty:
        parts.append(o2[COLS])
    orders = pd.concat(parts, ignore_index=True)

    # ── Clean ─────────────────────────────────────────────────────────────────
    orders["created_at"]       = pd.to_datetime(orders["created_at"], format="mixed", errors="coerce")
    orders["order_status"]     = orders["order_status"].apply(normalize_status)
    orders["customer_contact"] = orders["customer_contact"].apply(clean_phone)
    orders["platform"]         = orders["order_id"].apply(detect_platform)
    orders["restaurant_name"]  = orders["shop_id"].map(rest_name_map)
    orders["order_value"]      = pd.to_numeric(orders["order_value"],  errors="coerce")
    orders["discount"]         = pd.to_numeric(orders["discount"],     errors="coerce").fillna(0)

    # ── Date dimensions ───────────────────────────────────────────────────────
    orders["order_date"]       = orders["created_at"].dt.date
    orders["order_year"]       = orders["created_at"].dt.year
    orders["order_month"]      = orders["created_at"].dt.month
    orders["order_month_name"] = orders["created_at"].dt.strftime("%B")
    orders["order_week"]       = orders["created_at"].dt.isocalendar().week.astype("Int64")
    orders["order_hour"]       = orders["created_at"].dt.hour
    orders["order_dow"]        = orders["created_at"].dt.day_name()

    # ── Derived metrics ───────────────────────────────────────────────────────
    orders["net_revenue"]  = orders["order_value"] - orders["discount"]
    orders["is_cancelled"] = orders["order_status"] == "Cancelled"
    orders["has_coupon"]   = orders["coupon_value"].fillna(0) > 0

    orders = orders.drop_duplicates(subset=["order_id"])

    log(f"  fact_orders: {len(orders):,}")
    log(f"    gf_whatsapp:    {(orders['platform']=='gf_whatsapp').sum()}")
    log(f"    swayo_whatsapp: {(orders['platform']=='swayo_whatsapp').sum()}")
    log(f"    swayo_app:      {(orders['platform']=='swayo_app').sum()}")
    return orders


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — FACT: ORDER ITEMS
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_order_items(f1: pd.DataFrame, f2: pd.DataFrame) -> pd.DataFrame:
    i1 = f1[["Pos_Order_ID", "Item_Name"]].copy()
    i1.columns = ["order_id", "product_name"]

    i2 = pd.DataFrame()
    if not f2.empty:
        tmp = f2[["Order ID", "Item Names"]].copy()
        tmp.columns = ["order_id", "product_name"]
        tmp["product_name"] = tmp["product_name"].astype(str).str.split(",")
        tmp = tmp.explode("product_name")
        tmp["product_name"] = tmp["product_name"].str.strip()
        i2 = tmp[~tmp["order_id"].isin(set(i1["order_id"]))]

    items = pd.concat([i1, i2], ignore_index=True)
    items = items[items["product_name"].notna()]
    items = items[~items["product_name"].str.strip().str.lower().isin(["", "nan", "none"])]
    items["platform"] = items["order_id"].apply(detect_platform)

    log(f"  fact_order_items: {len(items):,}")
    return items


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — FACT: ORDER GEO  ← NEW TABLE
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_order_geo(
    f1: pd.DataFrame,
    f2: pd.DataFrame,
    restaurant_map: pd.DataFrame,
) -> pd.DataFrame:
    """
    One row per ORDER with location/distance data.

    Columns:
      order_id, platform, restaurant_name,
      restaurant_pincode  ← Seller_Pincode from data.csv
      customer_contact,
      customer_pincode    ← extracted from full address (data.csv Delivery_Pincode)
                            or directly from response CSV Delivery Pincode
      delivery_address    ← raw full address string (data.csv only)
      delivery_distance,
      order_date

    Notes:
      - data.csv  "Delivery_Pincode" column = FULL ADDRESS STRING (not a pincode)
        → we regex-extract the 6-digit pincode from it
        → we also keep the raw string as delivery_address
      - response CSV "Delivery Pincode" column = already a numeric pincode
    """
    rest_pincode_map = restaurant_map.set_index("shop_id")["seller_pincode"].to_dict()
    rest_name_map    = restaurant_map.set_index("shop_id")["restaurant_name"].to_dict()

    # ── f1: GF WhatsApp + Swayo WhatsApp ─────────────────────────────────────
    o1 = f1.copy()
    o1["shop_id"] = o1["onboarded_date"].apply(extract_shop_id_from_onboarded).fillna(
                    o1["Provider_ID"].astype(str).str.strip())
    o1["created_at"] = pd.to_datetime(o1["Created"], errors="coerce")

    # Extract 6-digit pincode from full address string
    o1["customer_pincode"]  = o1["Delivery_Pincode"].apply(extract_pincode_from_address)
    o1["delivery_address"]  = o1["Delivery_Pincode"].astype(str).str.strip()
    o1["delivery_address"]  = o1["delivery_address"].where(
                                o1["delivery_address"] != "nan", None)

    geo1 = o1.rename(columns={
        "Pos_Order_ID"    : "order_id",
        "customer_contact": "customer_contact",
        "delivery_distance": "delivery_distance",
    })[[
        "order_id", "shop_id", "customer_contact",
        "customer_pincode", "delivery_address",
        "delivery_distance", "created_at",
    ]].drop_duplicates(subset=["order_id"])

    # ── f2: Swayo App ─────────────────────────────────────────────────────────
    geo2 = pd.DataFrame()
    if not f2.empty:
        o2 = f2.copy()
        o2["created_at"] = pd.to_datetime(o2["Created"], format="mixed", errors="coerce")
        # response CSV has numeric Delivery Pincode already
        o2["customer_pincode"] = o2["Delivery Pincode"].apply(
            lambda x: str(int(x)) if pd.notna(x) and x != 0 else None
        )
        o2["delivery_address"] = None  # not available in response CSV

        geo2 = o2.rename(columns={
            "Order ID"         : "order_id",
            "Shop ID"          : "shop_id",
            "Customer Contact" : "customer_contact",
            "Delivery Distance": "delivery_distance",
        })[[
            "order_id", "shop_id", "customer_contact",
            "customer_pincode", "delivery_address",
            "delivery_distance", "created_at",
        ]].drop_duplicates(subset=["order_id"])

        # Remove rows already in geo1
        geo2 = geo2[~geo2["order_id"].isin(set(geo1["order_id"]))]

    # ── Union ─────────────────────────────────────────────────────────────────
    parts = [geo1]
    if not geo2.empty:
        parts.append(geo2)
    geo = pd.concat(parts, ignore_index=True)

    # ── Enrich ───────────────────────────────────────────────────────────────
    geo["platform"]            = geo["order_id"].apply(detect_platform)
    geo["restaurant_name"]     = geo["shop_id"].map(rest_name_map)
    geo["restaurant_pincode"]  = geo["shop_id"].map(rest_pincode_map).apply(
        lambda x: str(int(x)) if pd.notna(x) else None
    )
    geo["customer_contact"]    = geo["customer_contact"].apply(clean_phone)
    geo["order_date"]          = pd.to_datetime(geo["created_at"], errors="coerce").dt.date
    geo["delivery_distance"]   = pd.to_numeric(geo["delivery_distance"], errors="coerce")

    # Drop internal shop_id from final output (restaurant_name already added)
    geo = geo[[
        "order_id", "platform", "restaurant_name", "restaurant_pincode",
        "customer_contact", "customer_pincode", "delivery_address",
        "delivery_distance", "order_date",
    ]]

    log(f"  fact_order_geo: {len(geo):,} rows")
    log(f"    with restaurant_pincode: {geo['restaurant_pincode'].notna().sum()}")
    log(f"    with customer_pincode:   {geo['customer_pincode'].notna().sum()}")
    log(f"    with delivery_address:   {geo['delivery_address'].notna().sum()}")
    return geo


# ─────────────────────────────────────────────────────────────────────────────
# STEP 8 — FACT: FUNNEL (Swayo App — 45k rows, v3 logic preserved)
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_funnel_app(f3: pd.DataFrame) -> pd.DataFrame:
    """
    Swayo App funnel events.
    Key fix (from v3): format='mixed' handles two timestamp formats:
      "2026-02-13 23:08:09"              (rows 0-13999)
      "2026-02-26 01:33:19,979185"       (rows 14000+ — comma microseconds)
    PDP rows have no customer_contact BY DESIGN (anonymous browse).
    Only garbage dropped: PDP rows with literal "shop_id" string as Shop ID.
    """
    if f3.empty:
        return pd.DataFrame()

    df = f3.copy()
    df = df.rename(columns={
        "Action"         : "action",
        "Timestamp"      : "timestamp_raw",
        "Customer Number": "customer_number",
        "Shop ID"        : "shop_id",
    })

    # Fix comma-microseconds → parse with format=mixed
    df["timestamp"] = pd.to_datetime(
        df["timestamp_raw"].astype(str).str.replace(",", ".", regex=False),
        format="mixed",
        errors="coerce",
    )

    # Drop only genuine garbage
    bad_ts      = df["timestamp"].isna()
    garbage_pdp = (df["action"] == "PDP") & (df["shop_id"] == "shop_id")
    bad_action  = ~df["action"].isin(VALID_APP_FUNNEL)
    df = df[~(bad_ts | garbage_pdp | bad_action)].copy()

    df["customer_contact"] = df["customer_number"].apply(
        lambda x: clean_phone(x) if pd.notna(x) else None
    )
    df["event_date"]       = df["timestamp"].dt.date
    df["event_hour"]       = df["timestamp"].dt.hour
    df["event_month"]      = df["timestamp"].dt.month
    df["event_month_name"] = df["timestamp"].dt.strftime("%B")
    df["event_dow"]        = df["timestamp"].dt.day_name()
    df["action_order"]     = df["action"].map(APP_ACTION_ORDER)
    df["funnel_source"]    = "swayo_app"

    df = df.reset_index(drop=True)
    df["row_id"] = df.index
    last = get_last_funnel_row("app_funnel")
    new  = df[df["row_id"] > last].copy()

    log(f"  fact_funnel (app): {len(new):,} new rows (total clean: {len(df):,})")
    log(f"    actions: {new['action'].value_counts().to_dict()}")
    return new


# ─────────────────────────────────────────────────────────────────────────────
# STEP 9 — FACT: FUNNEL WA  ← NEW TABLE
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_funnel_wa(
    f4: pd.DataFrame,
    restaurant_map: pd.DataFrame,
) -> pd.DataFrame:
    """
    WhatsApp funnel events from datewise_funnelanalysis_wa*.csv

    Schema of raw file:
      timestamp       ISO8601 with +05:30 timezone
      action          VIEW_CATALOG | TOFU | VIEW_CART | CHECKOUT | ORDER | QUERY
      campaign_id     campaign identifier
      customer_code   shop code (e.g. gff12390) — NOT customer phone
      customer_name   customer name (may be null)
      customer_number 12-digit float (91XXXXXXXXXX.0) → clean to 10 digits
      misc            message text (hi, hello, etc.) — mostly NaN
      shop_id         GFF shop ID (same format as data.csv Provider_ID)
      shop_name       restaurant name in WA funnel (may differ from canonical)
      status          always SUCCESS

    Key decisions:
      - customer_code is a SHOP code (33 unique), NOT a customer identifier
      - shop_name enriched to canonical restaurant_name via shop_id join
      - misc (greetings) kept as wa_message for behavioral analysis
      - timestamp: utc=True strips timezone, converts to UTC
    """
    if f4.empty:
        return pd.DataFrame()

    df = f4.copy()

    # ── Parse timestamp (ISO8601 with +05:30 timezone) ────────────────────────
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["timestamp"] = df["timestamp"].dt.tz_localize(None)  # remove tz info for MySQL

    bad_ts     = df["timestamp"].isna()
    bad_action = ~df["action"].isin(VALID_WA_FUNNEL)
    df = df[~(bad_ts | bad_action)].copy()

    # ── Clean customer phone ──────────────────────────────────────────────────
    df["customer_contact"] = df["customer_number"].apply(clean_phone)

    # ── Enrich shop_id → canonical restaurant_name ────────────────────────────
    rest_name_map = restaurant_map.set_index("shop_id")["restaurant_name"].to_dict()
    df["restaurant_name"] = df["shop_id"].map(rest_name_map)

    # Where shop_id not in map, use shop_name from WA funnel file (fallback)
    mask_no_match = df["restaurant_name"].isna() & df["shop_name"].notna()
    df.loc[mask_no_match, "restaurant_name"] = df.loc[mask_no_match, "shop_name"].str.strip()

    # ── Date/time columns ─────────────────────────────────────────────────────
    df["event_date"]       = df["timestamp"].dt.date
    df["event_hour"]       = df["timestamp"].dt.hour
    df["event_month"]      = df["timestamp"].dt.month
    df["event_month_name"] = df["timestamp"].dt.strftime("%B")
    df["event_dow"]        = df["timestamp"].dt.day_name()
    df["action_order"]     = df["action"].map(WA_ACTION_ORDER)
    df["funnel_source"]    = "whatsapp"

    # ── Select final columns ──────────────────────────────────────────────────
    df = df.rename(columns={
        "campaign_id"   : "campaign_id",
        "customer_code" : "shop_code",     # renamed for clarity (this is a shop code)
        "customer_name" : "customer_name",
        "misc"          : "wa_message",
        "shop_id"       : "shop_id",
        "status"        : "status",
    })

    result = df[[
        "timestamp", "action", "action_order", "funnel_source",
        "campaign_id", "shop_code", "shop_id", "restaurant_name",
        "customer_contact", "customer_name",
        "wa_message",
        "event_date", "event_hour", "event_month", "event_month_name", "event_dow",
        "status",
    ]].copy()

    # ── Incremental ───────────────────────────────────────────────────────────
    result = result.reset_index(drop=True)
    result["row_id"] = result.index
    last = get_last_funnel_row("wa_funnel")
    new  = result[result["row_id"] > last].copy()

    log(f"  fact_funnel_wa: {len(new):,} new rows (total clean: {len(result):,})")
    log(f"    actions: {new['action'].value_counts().to_dict()}")
    log(f"    unique customers: {new['customer_contact'].nunique()}")
    log(f"    unique restaurants: {new['restaurant_name'].nunique()}")
    return new


# ─────────────────────────────────────────────────────────────────────────────
# STEP 10 — FACT: CAMPAIGNS  ← NEW
# ─────────────────────────────────────────────────────────────────────────────

def extract_campaign_name(filepath: str) -> str:
    """
    Derive campaign_name from filename — the filename IS the campaign identifier.

    Supported patterns (all → strip extension + report suffix):
      mfc_swayowhatsapp_xlsx_-_report.csv   → "mfc_swayowhatsapp"
      just_fresh_point_march_-_report.csv   → "just_fresh_point_march"
      shawarma_house_diwali.csv             → "shawarma_house_diwali"
      food_court_weekend_offer_-_report.csv → "food_court_weekend_offer"
    """
    import re as _re
    stem = os.path.basename(filepath)
    stem = _re.sub(r"\.csv$", "", stem, flags=_re.IGNORECASE)
    stem = _re.sub(r"_xlsx_-_report$", "", stem)   # pattern 1
    stem = _re.sub(r"_-_report$",      "", stem)   # pattern 2
    stem = _re.sub(r"_report$",        "", stem)   # pattern 3
    stem = _re.sub(r"_+", "_", stem).strip("_")
    return stem


def build_fact_campaigns(campaign_files: list) -> pd.DataFrame:
    """
    Load all campaign report CSVs.
    campaign_name is extracted from each filename — no manual input needed.
    Columns:
      campaign_name, campaign_id, mobile_number, scheduled_date, scheduled_time,
      scheduled_at, sent_at, delivered_at, read_at,
      is_sent, is_delivered, is_read, delivery_status,
      pitch_response, loaded_at
    """
    if not campaign_files:
        log("  fact_campaigns: no campaign report files found", "warning")
        return pd.DataFrame()

    all_frames = []

    for filepath in campaign_files:
        campaign_name = extract_campaign_name(filepath)
        log(f"  Loading campaign: '{campaign_name}' ← {os.path.basename(filepath)}")

        try:
            df = pd.read_csv(filepath, low_memory=False)
            df.columns = df.columns.str.strip()
        except Exception as e:
            log(f"  ERROR reading {filepath}: {e}", "error")
            continue

        # ── Rename columns to standard schema ─────────────────────────────────
        df = df.rename(columns={
            "Campaign Id"       : "campaign_id",
            "Mobile Number"     : "mobile_number_raw",
            "Scheduled Date"    : "scheduled_date",
            "Scheduled Time"    : "scheduled_time",
            "Sent"              : "sent_at",
            "Delivered"         : "delivered_at",
            "Read"              : "read_at",
            "1st Pitch Response": "pitch_response",
        })

        # ── campaign_name from filename ────────────────────────────────────────
        df["campaign_name"] = campaign_name

        # ── Clean mobile number → 10-digit ────────────────────────────────────
        df["mobile_number"] = df["mobile_number_raw"].apply(clean_phone)

        # ── Parse datetimes ───────────────────────────────────────────────────
        df["sent_at"]      = pd.to_datetime(df["sent_at"],      errors="coerce")
        df["delivered_at"] = pd.to_datetime(df["delivered_at"], errors="coerce")
        df["read_at"]      = pd.to_datetime(df["read_at"],      errors="coerce")

        # ── Fix bad scheduled_date values (e.g. "2018-07-03" outlier) ─────────
        df["scheduled_date"] = pd.to_datetime(df["scheduled_date"], errors="coerce").dt.date
        # If scheduled_date is clearly wrong (before 2020), replace with sent_at date
        df["scheduled_date"] = df.apply(
            lambda r: r["sent_at"].date() if (
                pd.notna(r["sent_at"]) and
                pd.notna(r["scheduled_date"]) and
                r["scheduled_date"].year < 2020
            ) else r["scheduled_date"],
            axis=1,
        )

        # ── Combined scheduled_at datetime ────────────────────────────────────
        def combine_sched(row):
            try:
                return pd.Timestamp(f"{row['scheduled_date']} {row['scheduled_time']}")
            except Exception:
                return pd.NaT
        df["scheduled_at"] = df.apply(combine_sched, axis=1)

        # ── Boolean flags ─────────────────────────────────────────────────────
        df["is_sent"]      = df["sent_at"].notna().astype(int)
        df["is_delivered"] = df["delivered_at"].notna().astype(int)
        df["is_read"]      = df["read_at"].notna().astype(int)

        # ── Delivery status (best status achieved) ────────────────────────────
        def delivery_status(row):
            if row["is_read"]:      return "Read"
            if row["is_delivered"]: return "Delivered"
            if row["is_sent"]:      return "Sent"
            return "Failed"
        df["delivery_status"] = df.apply(delivery_status, axis=1)

        # ── Pipeline load timestamp ───────────────────────────────────────────
        df["loaded_at"] = pd.Timestamp.now().floor("s")

        # ── Final columns ─────────────────────────────────────────────────────
        keep = [
            "campaign_name", "campaign_id", "mobile_number",
            "scheduled_date", "scheduled_time", "scheduled_at",
            "sent_at", "delivered_at", "read_at",
            "is_sent", "is_delivered", "is_read",
            "delivery_status", "pitch_response", "loaded_at",
        ]
        all_frames.append(df[keep])

    if not all_frames:
        return pd.DataFrame()

    result = pd.concat(all_frames, ignore_index=True)
    log(f"  fact_campaigns: {len(result):,} rows from {len(campaign_files)} file(s)")
    log(f"    campaigns: {result['campaign_name'].unique().tolist()}")
    log(f"    delivery_status: {result['delivery_status'].value_counts().to_dict()}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# STEP 11 — AGG: CAMPAIGN PERFORMANCE  ← NEW
# ─────────────────────────────────────────────────────────────────────────────

def build_agg_campaign_performance(
    fact_campaigns: pd.DataFrame,
    fact_funnel_wa: pd.DataFrame,
) -> pd.DataFrame:
    """
    Per campaign_id delivery KPIs + post-campaign order conversion.

    Conversion logic:
      A recipient is counted as "converted" if they appear in fact_funnel_wa
      with action='ORDER' within 24 hours of their read_at timestamp.

    This answers: "After this campaign was sent, how many recipients
    actually placed an order within the next 24 hours?"

    Join chain used:
      fact_campaigns.mobile_number = fact_funnel_wa.customer_contact
      fact_campaigns.campaign_id   = fact_funnel_wa.campaign_id (if available)
      fact_funnel_wa.action        = 'ORDER'
      fact_funnel_wa.timestamp     BETWEEN read_at AND read_at + 24h
    """
    if fact_campaigns is None or fact_campaigns.empty:
        return pd.DataFrame()

    # ── Per campaign_id delivery stats ────────────────────────────────────────
    agg = fact_campaigns.groupby(
        ["campaign_name", "campaign_id"], dropna=False
    ).agg(
        scheduled_date    = ("scheduled_date",  "first"),
        total_recipients  = ("mobile_number",   "count"),
        sent_count        = ("is_sent",         "sum"),
        delivered_count   = ("is_delivered",    "sum"),
        read_count        = ("is_read",         "sum"),
    ).reset_index()

    agg["sent_rate_pct"]      = (agg["sent_count"]      / agg["total_recipients"].replace(0, np.nan) * 100).round(1)
    agg["delivered_rate_pct"] = (agg["delivered_count"] / agg["total_recipients"].replace(0, np.nan) * 100).round(1)
    agg["read_rate_pct"]      = (agg["read_count"]      / agg["total_recipients"].replace(0, np.nan) * 100).round(1)

    # ── Post-campaign order conversion ────────────────────────────────────────
    agg["orders_after_24h"]    = 0
    agg["conversion_rate_pct"] = 0.0

    if fact_funnel_wa is not None and not fact_funnel_wa.empty:
        # Get all ORDER events from WA funnel
        wa_orders = fact_funnel_wa[
            fact_funnel_wa["action"] == "ORDER"
        ][["customer_contact", "timestamp"]].copy()
        wa_orders = wa_orders.dropna(subset=["customer_contact", "timestamp"])
        wa_orders["timestamp"] = pd.to_datetime(wa_orders["timestamp"], errors="coerce")

        # For each campaign_id, find recipients who read the message
        # and placed an ORDER within 24h of reading
        conv_counts = {}
        for _, camp_row in agg.iterrows():
            cid = camp_row["campaign_id"]
            camp_readers = fact_campaigns[
                (fact_campaigns["campaign_id"] == cid) &
                (fact_campaigns["is_read"] == 1) &
                (fact_campaigns["mobile_number"].notna()) &
                (fact_campaigns["read_at"].notna())
            ][["mobile_number", "read_at"]].copy()

            if camp_readers.empty:
                conv_counts[cid] = 0
                continue

            # Join readers to WA ORDER events on mobile_number
            joined = camp_readers.merge(
                wa_orders,
                left_on="mobile_number",
                right_on="customer_contact",
                how="inner",
            )
            # Keep only orders within 24 hours AFTER reading
            joined["read_at"]   = pd.to_datetime(joined["read_at"],  errors="coerce")
            joined["hours_gap"] = (joined["timestamp"] - joined["read_at"]).dt.total_seconds() / 3600
            converted = joined[(joined["hours_gap"] >= 0) & (joined["hours_gap"] <= 24)]
            conv_counts[cid] = converted["mobile_number"].nunique()

        agg["orders_after_24h"]    = agg["campaign_id"].map(conv_counts).fillna(0).astype(int)
        agg["conversion_rate_pct"] = (
            agg["orders_after_24h"] / agg["read_count"].replace(0, np.nan) * 100
        ).round(1).fillna(0)

    log(f"  agg_campaign_performance: {len(agg):,} rows")
    for _, r in agg.iterrows():
        log(f"    [{r['campaign_name']}] {r['campaign_id']}: "
            f"recipients={r['total_recipients']} sent={r['sent_count']} "
            f"delivered={r['delivered_count']} read={r['read_count']} "
            f"orders_after_24h={r['orders_after_24h']} "
            f"conversion={r['conversion_rate_pct']}%")
    return agg




def build_agg_platform_daily(fact_orders: pd.DataFrame) -> pd.DataFrame:
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
    log(f"  agg_platform_daily: {len(agg):,}")
    return agg


def build_agg_restaurant_daily(fact_orders: pd.DataFrame) -> pd.DataFrame:
    agg = fact_orders.groupby(
        ["restaurant_name", "shop_id", "platform", "order_date"], dropna=False
    ).agg(
        gmv             = ("order_value",  "sum"),
        net_revenue     = ("net_revenue",  "sum"),
        order_count     = ("order_id",     "count"),
        avg_order_value = ("order_value",  "mean"),
        discount_given  = ("discount",     "sum"),
        cancelled_count = ("is_cancelled", "sum"),
    ).reset_index()
    agg["cancellation_rate"] = (
        agg["cancelled_count"] / agg["order_count"].replace(0, 1) * 100
    ).round(2)
    log(f"  agg_restaurant_daily: {len(agg):,}")
    return agg


def build_agg_funnel_conversion(fact_funnel: pd.DataFrame) -> pd.DataFrame:
    if fact_funnel.empty:
        return pd.DataFrame()
    pivot = fact_funnel.groupby(
        ["shop_id", "event_date", "action"], dropna=False
    ).size().reset_index(name="n")
    wide = pivot.pivot_table(
        index=["shop_id", "event_date"], columns="action",
        values="n", fill_value=0,
    ).reset_index()
    wide.columns.name = None
    for c in ["PDP", "PLP", "VIEW_CART", "CHECKOUT", "ORDER"]:
        if c not in wide.columns: wide[c] = 0
    wide = wide.rename(columns={
        "PDP": "pdp_views", "PLP": "plp_views", "VIEW_CART": "cart_views",
        "CHECKOUT": "checkouts", "ORDER": "orders_placed",
    })
    def rate(n, d):
        return (n / d.replace(0, np.nan) * 100).round(2)
    wide["plp_to_cart_rate"]        = rate(wide["cart_views"],    wide["plp_views"])
    wide["cart_to_checkout_rate"]   = rate(wide["checkouts"],     wide["cart_views"])
    wide["checkout_to_order_rate"]  = rate(wide["orders_placed"], wide["checkouts"])
    wide["overall_conversion_rate"] = rate(wide["orders_placed"], wide["plp_views"])
    log(f"  agg_funnel_conversion: {len(wide):,}")
    return wide


def build_agg_customer_behavior(fact_orders: pd.DataFrame) -> pd.DataFrame:
    agg = fact_orders[fact_orders["customer_contact"].notna()].groupby(
        "customer_contact", dropna=False
    ).agg(
        total_orders     = ("order_id",     "count"),
        total_gmv        = ("order_value",  "sum"),
        avg_order_value  = ("order_value",  "mean"),
        total_discount   = ("discount",     "sum"),
        first_order_date = ("order_date",   "min"),
        last_order_date  = ("order_date",   "max"),
        platforms_used   = ("platform",     "nunique"),
        cancelled_orders = ("is_cancelled", "sum"),
        coupon_usage     = ("has_coupon",   "sum"),
    ).reset_index()
    def seg(n):
        if n >= 10: return "VIP"
        if n >= 5:  return "Loyal"
        if n >= 2:  return "Repeat"
        return "One-time"
    agg["customer_segment"] = agg["total_orders"].apply(seg)
    log(f"  agg_customer_behavior: {len(agg):,}")
    log(f"    {agg['customer_segment'].value_counts().to_dict()}")
    return agg


# ─────────────────────────────────────────────────────────────────────────────
# STEP 11 — ARCHIVE
# ─────────────────────────────────────────────────────────────────────────────

def archive_files():
    patterns = [
        f"{RAW}/data*.csv",
        f"{RAW}/response*.csv",
        f"{RAW}/funnelanalysis*.csv",
        f"{RAW}/datewise_funnelanalysis_wa*.csv",
        f"{RAW}/*_report.csv",    # ← campaign report files
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
    log("PIPELINE v5.0 — START")
    log("=" * 65)

    # 1. Load
    log("\n[1/12] Loading files...")
    result = load_files()
    if result is None:
        return
    f1, f2, f3, f4, campaign_files = result

    # 2. Restaurant map (no lookup file)
    log("\n[2/12] Building restaurant map from source files...")
    restaurant_map = build_restaurant_map(f1, f2)

    # 3. Dimensions
    log("\n[3/12] Building dimension tables...")
    dim_restaurants = build_dim_restaurants(restaurant_map)
    dim_customers   = build_dim_customers(f1, f2)

    # 4. Facts
    log("\n[4/12] Building fact tables...")
    fact_orders      = build_fact_orders(f1, f2, restaurant_map)
    fact_order_items = build_fact_order_items(f1, f2)
    fact_order_geo   = build_fact_order_geo(f1, f2, restaurant_map)

    # 5. Funnels
    log("\n[5/12] Building funnel tables...")
    fact_funnel_app = build_fact_funnel_app(f3)
    fact_funnel_wa  = build_fact_funnel_wa(f4, restaurant_map)

    # 6. Campaigns
    log("\n[6/12] Building campaign tables...")
    fact_campaigns      = build_fact_campaigns(campaign_files)
    agg_campaign_perf   = build_agg_campaign_performance(fact_campaigns, fact_funnel_wa)

    # 7. Aggregations
    log("\n[7/12] Building aggregation tables...")
    agg_platform   = build_agg_platform_daily(fact_orders)
    agg_restaurant = build_agg_restaurant_daily(fact_orders)
    agg_funnel     = build_agg_funnel_conversion(fact_funnel_app)
    agg_customers  = build_agg_customer_behavior(fact_orders)

    # 8. Load to MySQL
    log("\n[8/12] Loading to MySQL...")
    load_to_mysql(dim_restaurants,    "dim_restaurants",          if_exists="replace")
    load_to_mysql(dim_customers,      "dim_customers",            if_exists="replace")
    load_to_mysql(fact_orders,        "fact_orders",              if_exists="append")
    load_to_mysql(fact_order_items,   "fact_order_items",         if_exists="append")
    load_to_mysql(fact_order_geo,     "fact_order_geo",           if_exists="append")
    load_to_mysql(fact_funnel_app,    "fact_funnel",              if_exists="append")
    load_to_mysql(fact_funnel_wa,     "fact_funnel_wa",           if_exists="append")
    load_to_mysql(fact_campaigns,     "fact_campaigns",           if_exists="append")
    load_to_mysql(agg_platform,       "agg_platform_daily",       if_exists="replace")
    load_to_mysql(agg_restaurant,     "agg_restaurant_daily",     if_exists="replace")
    load_to_mysql(agg_funnel,         "agg_funnel_conversion",    if_exists="replace")
    load_to_mysql(agg_customers,      "agg_customer_behavior",    if_exists="replace")
    load_to_mysql(agg_campaign_perf,  "agg_campaign_performance", if_exists="replace")

    # 9. Checkpoints
    log("\n[9/12] Saving incremental checkpoints...")
    save_last_funnel_row(fact_funnel_app, "app_funnel")
    save_last_funnel_row(fact_funnel_wa,  "wa_funnel")

    # 10. Archive
    log("\n[10/12] Archiving raw files...")
    archive_files()

    # 11. Summary
    log("\n[11/12] PIPELINE SUMMARY")
    log("─" * 55)
    log(f"  dim_restaurants        : {len(dim_restaurants):>7,}  (no lookup file)")
    log(f"  dim_customers          : {len(dim_customers):>7,}")
    log(f"  fact_orders            : {len(fact_orders):>7,}")
    log(f"    gf_whatsapp          : {(fact_orders['platform']=='gf_whatsapp').sum():>7,}")
    log(f"    swayo_whatsapp       : {(fact_orders['platform']=='swayo_whatsapp').sum():>7,}")
    log(f"    swayo_app            : {(fact_orders['platform']=='swayo_app').sum():>7,}")
    log(f"  fact_order_items       : {len(fact_order_items):>7,}")
    log(f"  fact_order_geo         : {len(fact_order_geo):>7,}")
    log(f"  fact_funnel (app)      : {len(fact_funnel_app):>7,}")
    log(f"  fact_funnel_wa         : {len(fact_funnel_wa):>7,}")
    log(f"  fact_campaigns         : {len(fact_campaigns) if not fact_campaigns.empty else 0:>7,}  ← NEW")
    log(f"  agg_platform_daily     : {len(agg_platform):>7,}")
    log(f"  agg_restaurant_daily   : {len(agg_restaurant):>7,}")
    log(f"  agg_funnel_conversion  : {len(agg_funnel):>7,}")
    log(f"  agg_customer_behavior  : {len(agg_customers):>7,}")
    log(f"  agg_campaign_performance: {len(agg_campaign_perf) if not agg_campaign_perf.empty else 0:>6,}  ← NEW")
    log(f"\n  TOTAL TABLES: 13  |  ✅ PIPELINE v5.0 COMPLETED")
    log("=" * 65 + "\n")


if __name__ == "__main__":
    run_pipeline()

# Run:  python3 scripts/pipeline.py