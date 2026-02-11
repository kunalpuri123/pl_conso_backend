import pandas as pd
import numpy as np
import re
from pathlib import Path
import sys
from datetime import datetime

print("=== PDP CHECKLIST STARTED ===", flush=True)

# ================= ARGUMENTS =================
if len(sys.argv) != 5:
    print("Usage: python pdp_check.py <CRAWL_OUTPUT> <CRAWL_INPUT> <MASTER_FILE> <OUTPUT_FILE>")
    sys.exit(1)

OP_FILE = Path(sys.argv[1])   # crawl output to be checked
IP_FILE = Path(sys.argv[2])   # crawl input (consolidated)
MASTER_FILE = Path(sys.argv[3])
OUTPUT_FILE = Path(sys.argv[4])

print(f"📄 Output File : {OP_FILE}")
print(f"📄 Input File  : {IP_FILE}")
print(f"📄 Master File : {MASTER_FILE}")
print(f"📄 Result File : {OUTPUT_FILE}")

for fp, label in [(OP_FILE, "Output"), (IP_FILE, "Input"), (MASTER_FILE, "Master")]:
    if not fp.exists():
        print(f"❌ {label} file not found: {fp}")
        sys.exit(1)

# ================= FILE HELPERS =================
def read_file(fp: Path) -> pd.DataFrame:
    if fp.suffix == ".tsv":
        try:
            return pd.read_csv(fp, sep="\t", dtype=str, encoding="utf-8", keep_default_na=False)
        except UnicodeDecodeError:
            print(f"⚠️ WARNING: {fp.name} is not UTF-8. Reading as latin1 (may corrupt data).")
            return pd.read_csv(fp, sep="\t", dtype=str, encoding="latin1", keep_default_na=False)
    if fp.suffix == ".csv":
        return pd.read_csv(fp, dtype=str, keep_default_na=False)
    return pd.read_excel(fp, dtype=str, keep_default_na=False)

def is_na_text(val: str) -> bool:
    if val is None:
        return True
    s = str(val).strip().lower()
    return s in {"", "n/a", "na", "null", "nan"}

def is_not_available(val: str) -> bool:
    if val is None:
        return False
    return str(val).strip().lower() == "not available"

def series_is_na(s: pd.Series) -> pd.Series:
    return s.isna() | s.astype(str).str.strip().str.lower().isin({"", "n/a", "na", "null", "nan"})

# ================= LOAD FILES =================
df = read_file(OP_FILE)
ip_df = read_file(IP_FILE)
master_df = read_file(MASTER_FILE)

# Normalize column names
df.columns = [c.strip().lower() for c in df.columns]
ip_df.columns = [c.strip().lower() for c in ip_df.columns]
master_df.columns = [c.strip().lower() for c in master_df.columns]

# ================= REQUIRED BASE COLUMNS =================
required_cols = ["scope", "rname", "country"]
for col in required_cols:
    if col not in df.columns:
        print(f"❌ Output file missing '{col}' column")
        sys.exit(1)

scopes_in_output = set(df["scope"].dropna().astype(str).str.strip())
print("🔎 Scopes found in output:", scopes_in_output)

# Filter input by scopes found in output
if "scope" in ip_df.columns:
    ip_df = ip_df[ip_df["scope"].isin(scopes_in_output)]

print(f"✅ Files loaded | OP Rows: {len(df)} | IP Rows: {len(ip_df)} | Master Rows: {len(master_df)}")

# ================= MASTER MAP (scope → rname/country) =================
valid_scope_rname = set()
valid_scope_country = set()

for _, r in master_df.iterrows():
    scope = str(r.get("scope", "")).strip()
    rname = str(r.get("rname", "")).strip()
    country = str(r.get("country", "")).strip()
    if scope and rname:
        valid_scope_rname.add((scope, rname))
    if scope and country:
        valid_scope_country.add((scope, country))

# ================= RNAME / COUNTRY CHECK =================
scope_s = df["scope"].astype(str).str.strip()
rname_s = df["rname"].astype(str).str.strip()
country_s = df["country"].astype(str).str.strip()

rname_pairs = pd.Series(list(zip(scope_s, rname_s)))
country_pairs = pd.Series(list(zip(scope_s, country_s)))

df["rname_check"] = np.where(rname_pairs.isin(valid_scope_rname), "PASS", "FAIL")
df["country_check"] = np.where(country_pairs.isin(valid_scope_country), "PASS", "FAIL")

# ================= ROW COUNT CHECK (per scope + rname) =================
row_key = list(zip(scope_s, rname_s))
op_counts = pd.Series(row_key).value_counts().to_dict()

if "scope" in ip_df.columns and "rname" in ip_df.columns:
    ip_key = list(zip(ip_df["scope"].astype(str).str.strip(), ip_df["rname"].astype(str).str.strip()))
    ip_counts = pd.Series(ip_key).value_counts().to_dict()
else:
    ip_counts = {}

expected_counts = [ip_counts.get(k, 0) for k in row_key]
actual_counts = [op_counts.get(k, 0) for k in row_key]

df["row_count_expected"] = expected_counts
df["row_count_actual"] = actual_counts
df["row_count_check"] = np.where(df["row_count_expected"] == df["row_count_actual"], "PASS", "FAIL")

# ================= DATE CHECK =================
today = datetime.today().strftime("%Y-%m-%d")
if "date" in df.columns:
    df["date_check"] = df["date"].apply(lambda x: "PASS" if str(x).startswith(today) else "FAIL")

# ================= PRICE / STATUS CHECK =================
reg = df.get("regularprice", pd.Series([""] * len(df))).astype(str).str.strip()
final = df.get("finalprice", pd.Series([""] * len(df))).astype(str).str.strip()
markdown = df.get("markdown_price", pd.Series([""] * len(df))).astype(str).str.strip()
item_status = df.get("item_status", pd.Series([""] * len(df))).astype(str).str.strip()

reg_na = reg.apply(is_na_text) | reg.apply(is_not_available)
final_na = final.apply(is_na_text) | final.apply(is_not_available)

df["regular_final_match"] = np.where(reg == final, "TRUE", "FALSE")
df["price_rule_check"] = "PASS"

# Case 1: regular == final and both are NA/Not available
mask_na_eq = (reg == final) & reg_na & final_na
df.loc[mask_na_eq & (item_status != final), "price_rule_check"] = "FAIL"
df.loc[mask_na_eq & (markdown != final), "price_rule_check"] = "FAIL"

# Case 2: regular == final and NOT NA
mask_eq = (reg == final) & ~mask_na_eq
df.loc[mask_eq & (item_status != "R"), "price_rule_check"] = "FAIL"
df.loc[mask_eq & (~markdown.apply(is_na_text)), "price_rule_check"] = "FAIL"

# Case 3: regular != final
mask_neq = reg != final
df.loc[mask_neq & (item_status != "M"), "price_rule_check"] = "FAIL"
df.loc[mask_neq & (markdown.apply(is_na_text)), "price_rule_check"] = "FAIL"

# ================= STOCK / AVAILABILITY CHECK =================
stock_status = df.get("stock_status", pd.Series([""] * len(df))).astype(str).str.strip()
availability = df.get("availability", pd.Series([""] * len(df))).astype(str).str.strip()

df["availability_check"] = "PASS"

mask_in = stock_status == "In Stock"
mask_out = stock_status == "Out of Stock"

df.loc[mask_in & (availability != "Yes"), "availability_check"] = "FAIL"
df.loc[mask_out & (availability != "No"), "availability_check"] = "FAIL"

# If In Stock / Out of Stock, no "Not available" in key columns
def is_not_available_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower() == "not available"

na_cols = []
for c in ["regularprice", "finalprice", "markdown_price", "item_status", "rating", "review", "availability"]:
    if c in df.columns:
        na_cols.append(c)

if na_cols:
    na_mask = np.zeros(len(df), dtype=bool)
    for c in na_cols:
        na_mask |= is_not_available_series(df[c])
    df["stock_na_check"] = np.where((mask_in | mask_out) & na_mask, "FAIL", "PASS")
else:
    df["stock_na_check"] = "PASS"

# ================= RATING CHECK =================
rating = df.get("rating", pd.Series([""] * len(df))).astype(str).str.strip()
review = df.get("review", pd.Series([""] * len(df))).astype(str).str.strip()

df["rating_check"] = "PASS"

rating_is_na = rating.apply(is_na_text) | rating.apply(is_not_available)
rating_numeric_ok = rating.str.match(r"^[0-9]+(\\.[0-9]+)?$", na=False)

rating_val = pd.to_numeric(rating.where(rating_numeric_ok, np.nan), errors="coerce")
rating_in_range = (rating_val >= 0) & (rating_val <= 5)

review_val = pd.to_numeric(review, errors="coerce").fillna(0)

# If stock status is Not available, allow text rating
mask_stock_na = stock_status == "Not available"

# For non-Not-available stock, rating must be numeric 0-5 and no spaces
df.loc[~mask_stock_na & (~rating_numeric_ok | ~rating_in_range), "rating_check"] = "FAIL"

# If review > 0, rating must be present
df.loc[(review_val > 0) & rating_is_na, "rating_check"] = "FAIL"

# ================= FAILURE REASON =================
FAILURE_MESSAGE_MAP = {
    "rname_check": "rname should match as per master",
    "country_check": "country should match as per master",
    "row_count_check": "row count does not match crawl input for retailer",
    "date_check": "date should be current date",
    "price_rule_check": "price/status rule failed",
    "availability_check": "availability should match stock status (In Stock/Out of Stock)",
    "stock_na_check": "Not available found in columns while stock status is In Stock/Out of Stock",
    "rating_check": "rating format or value is invalid",
}

check_cols = [c for c in FAILURE_MESSAGE_MAP.keys() if c in df.columns]

df["failure_reason"] = ""
for col in check_cols:
    df.loc[df[col].eq("FAIL"), "failure_reason"] += FAILURE_MESSAGE_MAP[col] + " | "

df["failure_reason"] = df["failure_reason"].str.rstrip(" | ")
df["overall_status"] = np.where(df["failure_reason"] == "", "PASS", "FAIL")

# ================= OUTPUT =================
df.to_csv(OUTPUT_FILE.with_suffix(".csv"), index=False)

with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter", engine_kwargs={"options": {"strings_to_urls": False}}) as writer:
    df.to_excel(writer, sheet_name="PDP_Data", index=False)

print(f"✅ OUTPUT GENERATED: {OUTPUT_FILE}")
print("=== PDP CHECKLIST COMPLETED ===")
