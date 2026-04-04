import glob
import logging
import os
import shutil
import pandas as pd
from sqlalchemy import create_engine

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

RAW = "data/raw"
ARCHIVE = "data/archive"
PROCESSED = "data/processed"

os.makedirs("logs", exist_ok=True)
os.makedirs(ARCHIVE, exist_ok=True)
os.makedirs(PROCESSED, exist_ok=True)

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

engine = create_engine(
    "mysql+pymysql://root:Rahul1975@localhost/funnel_pipeline"
)

# --------------------------------------------------
# HELPER FUNCTIONS
# --------------------------------------------------

def load_to_mysql(df, table):
    if df is None or df.empty:
        print(f"{table}: No new rows to insert")
        return

    df.to_sql(table, engine, if_exists="append", index=False)
    print(f"{table}: Inserted {len(df)} rows")


# -------------------------
# FUNNEL TRACKING FUNCTIONS
# -------------------------

def get_last_processed_row():
    try:
        with open(f"{PROCESSED}/funnel_last_run.txt", "r") as f:
            return int(f.read())
    except:
        return -1


def update_last_processed_row(df):
    if not df.empty:
        last_row = df["row_id"].max()
        with open(f"{PROCESSED}/funnel_last_run.txt", "w") as f:
            f.write(str(last_row))


# --------------------------------------------------
# LOAD FILES
# --------------------------------------------------

def load_files():
    data_files = glob.glob(f"{RAW}/data*.csv")
    response_files = glob.glob(f"{RAW}/response*.csv")
    funnel_files = glob.glob(f"{RAW}/funnelanalysis*.csv")

    if not data_files:
        print("No new files to process. Pipeline exiting.")
        return None

    file1 = pd.read_csv(data_files[0], low_memory=False)
    file2 = pd.read_csv(response_files[0])
    file3 = pd.read_csv(funnel_files[0])
    lookup = pd.read_csv(f"{RAW}/restaurant_lookup.csv")

    file2.columns = file2.columns.str.strip()

    print("Files loaded")
    return file1, file2, file3, lookup


# --------------------------------------------------
# ORDERS
# --------------------------------------------------

def process_orders(file1, file2):

    file2 = file2.rename(columns={
        "Order ID": "Pos_Order_ID",
        "Amount Paid by Customer": "Total_Order_Value",
        "Customer Name": "customer_name",
        "Customer Contact": "customer_contact"
    })

    whatsapp = file1[file1["Pos_Order_ID"].str.startswith("GF", na=False)].copy()
    swayo_wa = file1[file1["Pos_Order_ID"].str.startswith("SWWA", na=False)].copy()
    swayo_app = file2[file2["Pos_Order_ID"].str.startswith("SWYO", na=False)].copy()

    for df in [whatsapp, swayo_wa, swayo_app]:
        df["Created"] = pd.to_datetime(df["Created"], errors="coerce")

    orders = pd.concat([
        whatsapp[["Pos_Order_ID","customer_name","customer_contact","Total_Order_Value","Created"]],
        swayo_wa[["Pos_Order_ID","customer_name","customer_contact","Total_Order_Value","Created"]],
        swayo_app[["Pos_Order_ID","customer_name","customer_contact","Total_Order_Value","Created"]]
    ])

    orders = orders.rename(columns={
        "Pos_Order_ID": "order_id",
        "Created": "created_at",
        "Total_Order_Value": "order_value"
    })

    orders = orders.drop_duplicates(subset=["order_id"])

    print("Orders processed:", len(orders))

    whatsapp_orders = orders[orders["order_id"].str.startswith("GF")]
    swayo_whatsapp_orders = orders[orders["order_id"].str.startswith("SWWA")]
    swayo_app_orders = orders[orders["order_id"].str.startswith("SWYO")]

    return orders, whatsapp_orders, swayo_whatsapp_orders, swayo_app_orders


# --------------------------------------------------
# ITEMS
# --------------------------------------------------

def process_items(file1, file2):

    file2.columns = file2.columns.str.strip()

    order_col = "Pos_Order_ID" if "Pos_Order_ID" in file2.columns else "Order ID"

    items1 = file1[["Pos_Order_ID","Item_Name"]].rename(
        columns={"Pos_Order_ID":"order_id","Item_Name":"product_name"}
    )

    items2 = file2[[order_col,"Item Names"]].copy()
    items2["Item Names"] = items2["Item Names"].str.split(",")
    items2 = items2.explode("Item Names")
    items2["Item Names"] = items2["Item Names"].str.strip()

    items2 = items2.rename(columns={
        order_col:"order_id",
        "Item Names":"product_name"
    })

    items = pd.concat([items1, items2])
    items = items[items["product_name"].notna()]

    print("Items processed:", len(items))
    return items


# --------------------------------------------------
# FUNNEL (ROW-BASED INCREMENTAL)
# --------------------------------------------------

def process_funnel(file3):

    print("Funnel before:", len(file3))

    file3 = file3.reset_index(drop=True)
    file3["row_id"] = file3.index

    last_row = get_last_processed_row()
    print("Last processed row:", last_row)

    file3 = file3[file3["row_id"] > last_row]

    print("New funnel rows:", len(file3))

    return file3


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def run_pipeline():

    print("Pipeline started")

    files = load_files()
    if files is None:
        return

    file1, file2, file3, lookup = files

    orders, whatsapp, swayo_wa, swayo_app = process_orders(file1, file2)
    items = process_items(file1, file2)
    funnel = process_funnel(file3)

    # LOAD
    load_to_mysql(orders, "orders_raw")
    load_to_mysql(whatsapp, "whatsapp")
    load_to_mysql(swayo_wa, "swayo_whatsapp")
    load_to_mysql(swayo_app, "swayo_app")
    load_to_mysql(items, "order_items_raw")
    load_to_mysql(funnel, "funnel_raw")

    # UPDATE LAST ROW
    update_last_processed_row(funnel)

    # ARCHIVE FILES
    files_to_move = (
        glob.glob(f"{RAW}/data*.csv") +
        glob.glob(f"{RAW}/response*.csv") +
        glob.glob(f"{RAW}/funnelanalysis*.csv")
    )

    for f in files_to_move:
        shutil.move(f, f"{ARCHIVE}/{os.path.basename(f)}")

    print("Pipeline completed successfully")


# --------------------------------------------------
# RUN
# --------------------------------------------------

if __name__ == "__main__":
    run_pipeline()
    
#                  