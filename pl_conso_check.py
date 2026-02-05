import pandas as pd
import numpy as np
import re
from pathlib import Path
import sys
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import difflib

print("=== PL CONSO AUTOMATION STARTED ===")

def closest_token_match(value, candidates):
    val_tokens = set(normalize_text(value).split())

    best = ""
    best_score = 0

    for c in candidates:
        cand_tokens = set(normalize_text(c).split())

        score = len(val_tokens & cand_tokens)  # word overlap

        if score > best_score:
            best_score = score
            best = c

    return best


# ================= ARGUMENT SUPPORT (NEW) =================
if len(sys.argv) != 5:
    print("Usage: python pl_conso_check.py <OP_FILE> <IP_FILE> <MASTER_FILE> <OUTPUT_FILE>")
    sys.exit(1)

OP_FILE = Path(sys.argv[1])
IP_FILE = Path(sys.argv[2])
MASTER_FILE = Path(sys.argv[3])
OUTPUT_FILE = Path(sys.argv[4])

print(f"📄 OP File     : {OP_FILE}")
print(f"📄 IP File     : {IP_FILE}")
print(f"📄 Master File : {MASTER_FILE}")
print(f"📄 Output File : {OUTPUT_FILE}")

if not OP_FILE.exists():
    print(f"❌ OP file not found: {OP_FILE}")
    sys.exit(1)

if not IP_FILE.exists():
    print(f"❌ IP file not found: {IP_FILE}")
    sys.exit(1)

if not MASTER_FILE.exists():
    print(f"❌ Master file not found: {MASTER_FILE}")
    sys.exit(1)

# ================= FILE HELPERS =================
def get_single_file(folder):
    files = list(folder.glob("*.tsv")) + list(folder.glob("*.xlsx")) + list(folder.glob("*.csv"))
    if not files:
        print(f"❌ No file found in {folder}")
        sys.exit(1)
    return files[0]

def read_file(fp):
    if fp.suffix == ".tsv":
        try:
            return pd.read_csv(fp, sep="\t", dtype=str, encoding="utf-8", keep_default_na=False)
        except UnicodeDecodeError:
            print(f"⚠️ WARNING: {fp.name} is not UTF-8. Reading as latin1 (may corrupt data).")
            return pd.read_csv(fp, sep="\t", dtype=str, encoding="latin1", keep_default_na=False)
    if fp.suffix == ".csv":
        return pd.read_csv(fp, dtype=str, keep_default_na=False)
    return pd.read_excel(fp, dtype=str, keep_default_na=False)

# ================= LOAD FILES =================
# ================= LOAD OUTPUT FIRST =================

df = read_file(OP_FILE)
ip_df = read_file(IP_FILE)
master_df = read_file(MASTER_FILE)


print(f"📄 OP File     : {OP_FILE}")
print(f"📄 IP File     : {IP_FILE}")
print(f"📄 Master File : {MASTER_FILE}")

df = read_file(OP_FILE)
ip_df = read_file(IP_FILE)
master_df = read_file(MASTER_FILE)

df.columns = [c.strip().lower() for c in df.columns]
ip_df.columns = [c.strip().lower() for c in ip_df.columns]
master_df.columns = [c.strip().lower() for c in master_df.columns]


df = read_file(OP_FILE)
master_df = read_file(MASTER_FILE)

# Normalize column names
df.columns = [c.strip().lower() for c in df.columns]
master_df.columns = [c.strip().lower() for c in master_df.columns]

if "scope" not in df.columns and "scope_name" in df.columns:
    print("ℹ️ Renaming scope_name -> scope in OP file")
    df = df.rename(columns={"scope_name": "scope"})

# ================= DETECT REQUIRED SCOPES =================
if "scope" not in df.columns:
    print("❌ Output file does not contain 'scope' column")
    sys.exit(1)

scopes_in_output = set(df["scope"].dropna().astype(str).str.strip())

print("🔎 Scopes found in output:", scopes_in_output)



# ================= MERGE ALL INPUTS =================
if ip_df is None or ip_df.empty:
    print("❌ IP file is empty or could not be read")
    sys.exit(1)

# =========================================================
# CONSOLIDATED INPUT → FILTER ONLY REQUIRED SCOPES
# =========================================================

# normalize scope column
if "scope" not in ip_df.columns and "scope_name" in ip_df.columns:
    ip_df = ip_df.rename(columns={"scope_name": "scope"})

if "scope" not in ip_df.columns:
    print("❌ IP file missing 'scope' column")
    sys.exit(1)

# filter only scopes that appear in OP (performance boost)
ip_df = ip_df[ip_df["scope"].isin(scopes_in_output)]

print(f"✅ IP rows after scope filtering: {len(ip_df)}")


# =========================================================
# BUILD IP URL MAP PER SCOPE (correct way)
# =========================================================

ip_scope_to_urls = {}

for _, r in ip_df.iterrows():
    scope = str(r.get("scope")).strip()   # ✅ correct
    url = str(r.get("url")).strip()

    if url and url.lower() not in {"", "n/a", "na", "null", "nan"}:
        ip_scope_to_urls.setdefault(scope, set()).add(url)


# ================= NORMALIZE COLUMN NAMES =================
df.columns = [c.strip().lower() for c in df.columns]
ip_df.columns = [c.strip().lower() for c in ip_df.columns]
master_df.columns = [c.strip().lower() for c in master_df.columns]

if "scope" not in master_df.columns and "scope_name" in master_df.columns:
    print("ℹ️ Renaming scope_name -> scope in MASTER file")
    master_df = master_df.rename(columns={"scope_name": "scope"})

print(f"✅ Files loaded | OP Rows: {len(df)} | IP Rows: {len(ip_df)} | Master Rows: {len(master_df)}")

USE_DIFFLIB = len(df) <= 8000
# USE_DIFFLIB = True
print(f"Difflib enabled: {USE_DIFFLIB}", flush=True)

# ================= extra column =================
# ================= SCHEMA VALIDATION (EXTRA COLUMN CHECK) =================

ALLOWED_COLUMNS = set([
    "productid","pid","rname","brand","mpn","skuid","skuvarient","pname","division",
    "category","department","class","subclass","country","image_url_small","image_url_large",
    "position","key","channel","evidence_url","keywords","day","month","year","date","edat",
    "purl","scope","sorted_by","week","top_category_lvmh","category_lvmh","sub_category_lvmh",
    "normalized_brand","product_page_url","total_result_count",
    "search_keyword_output_category","ip_search_keyword_category","ip_filter_by_category",
    "op_filter_by_category","listing_type",
    "buffer_column_1","buffer_column_2","buffer_column_3","buffer_column_4","buffer_column_5"
])

# Also allow columns generated by automation
AUTO_GENERATED_COLUMNS_PREFIX = (
    "productid_check","scope_check","rname_check","country_check","brand_check","pname_check",
    "keywords_ip_check","purl_ip_check","top_category_lvmh_ip_check","category_lvmh_ip_check",
    "sub_category_lvmh_ip_check","date_check","product_page_url_check","listing_type_check",
    "evidence_url_validation","position_validation_status","failure_reason","overall_status",
    "unique_key","position_count","max_position"
)
# ================= FIND EXTRA COLUMNS IN OP =================

df_cols = set(df.columns)

allowed_plus_auto = set(ALLOWED_COLUMNS)

# Allow auto-generated columns too
for c in df_cols:
    for prefix in AUTO_GENERATED_COLUMNS_PREFIX:
        if c.startswith(prefix):
            allowed_plus_auto.add(c)

extra_columns = sorted(list(df_cols - allowed_plus_auto))

extra_columns_df = pd.DataFrame({
    "extra_column_name": extra_columns
})

if extra_columns:
    print("❌ EXTRA COLUMNS FOUND IN OUTPUT FILE:")
    for c in extra_columns:
        print("   -", c)
else:
    print("✅ No extra columns found in output file.")

# ================= NA HANDLING =================
def is_na(val):
    if val is None:
        return True
    if isinstance(val, float) and pd.isna(val):
        return True
    s = str(val).strip().lower()
    return s in {"", "n/a", "na", "null", "nan"}

def raw_str(val):
    if val is None:
        return ""
    if isinstance(val, float) and pd.isna(val):
        return ""
    return str(val)

def visible(val):
    return repr(val)

# ================= STRICT INPUT CHECK =================
def exists_in_ip_verbose(val, ip_set, is_url=False):
    if is_na(val):
        return "PASS", "n/a", ""

    raw = raw_str(val)

    if is_url:
        v = normalize_url_for_compare(raw)
    else:
        v = raw

    if v in ip_set:
        return "PASS", "", ""

    if USE_DIFFLIB:
        close = difflib.get_close_matches(v, list(ip_set), n=1, cutoff=0.6)
        close = close[0] if close else ""
    else:
        close = ""

    return "FAIL", visible(raw), visible(close)


# ================= MASTER CHECK FUNCTION =================
def check_from_master_verbose(val, valid_set):
    if is_na(val):
        return "PASS", "n/a", ""

    v = str(val).strip()

    if v in valid_set:
        return "PASS", "", ""

    close = difflib.get_close_matches(v, list(valid_set), n=1, cutoff=0.6)
    close = close[0] if close else ""

    return "FAIL", v, close


def normalize_url_for_compare(url):
    if url is None:
        return ""
    if isinstance(url, float) and pd.isna(url):
        return ""
    s = str(url)
    return s.split("#")[0]   # remove fragment only, DO NOT strip spaces

# ================= BUILD INPUT SETS =================
ip_sets = {
    "keywords": set(raw_str(x) for x in ip_df["search_keyword"] if not is_na(x)),
    "purl": set(normalize_url_for_compare(x) for x in ip_df["url"] if not is_na(x)),
    "top_category_lvmh": set(raw_str(x) for x in ip_df["top_category_lvmh"] if not is_na(x)),
    "category_lvmh": set(raw_str(x) for x in ip_df["category_lvmh"] if not is_na(x)),
    "sub_category_lvmh": set(raw_str(x) for x in ip_df["sub_category_lvmh"] if not is_na(x)),
}


# ================= MASTER MAP =================
valid_scopes = set()
valid_scope_rname = set()
valid_scope_country = set()

for _, r in master_df.iterrows():
    scope = str(r["scope"]).strip()
    rname = str(r["rname"]).strip()
    country = str(r["country"]).strip()

    if scope:
        valid_scopes.add(scope)
    if scope and rname:
        valid_scope_rname.add((scope, rname))
    if scope and country:
        valid_scope_country.add((scope, country))

all_scopes = set(valid_scopes)

scope_to_rnames = {}
scope_to_countries = {}

for s, rn in valid_scope_rname:
    scope_to_rnames.setdefault(s, set()).add(rn)

for s, c in valid_scope_country:
    scope_to_countries.setdefault(s, set()).add(c)

# ================= PRODUCTID CHECK =================
def check_productid(pid, channel):
    # If productid is NA
    if is_na(pid):
        # Allow only if channel is App
        if str(channel).strip() == "App":
            return "PASS", ""
        else:
            return "FAIL", "productid is n/a but channel is not App"

    pid = str(pid)

    # Check format: exactly 3 or 4 parts
    if re.match(r"^[^_]+(_[^_]+){2,3}$", pid):
        return "PASS", ""

    return "FAIL", "productid format invalid"


# ================= UNIQUE KEY =================
unique_key_columns = [
    "rname", "top_category_lvmh", "category_lvmh",
    "sub_category_lvmh", "keywords", "channel","country"
]

def safe_key(v):
    if v is None:
        return "__NULL__"
    s = str(v).strip()
    if s == "":
        return "__BLANK__"
    return s

df["unique_key"] = df[unique_key_columns].agg(
    lambda row: "".join(safe_key(v) for v in row),
    axis=1
)




# ================= ROW LEVEL CHECKS =================
results = []

for _, row in df.iterrows():
    reasons = []
    res = {}

    status, reason = check_productid(row.get("productid"), row.get("channel"))
    res["productid_check"] = status

    scope = str(row.get("scope")).strip()
    rname = str(row.get("rname")).strip()
    country = str(row.get("country")).strip()

    sc, sm, _ = check_from_master_verbose(scope, all_scopes)
    res["scope_check"] = sc
    res["scope_missing"] = sm

    rc, rm, _ = check_from_master_verbose(rname, scope_to_rnames.get(scope, set()))
    res["rname_check"] = rc
    res["rname_missing"] = rm

    cc, cm, _ = check_from_master_verbose(country, scope_to_countries.get(scope, set()))
    res["country_check"] = cc
    res["country_missing"] = cm

    res["brand_check"] = "FAIL" if is_na(row.get("brand")) else "PASS"
    res["pname_check"] = "FAIL" if is_na(row.get("pname")) else "PASS"

    results.append(res)

df = pd.concat([df, pd.DataFrame(results)], axis=1)

# ================= POSITION CHECK =================
df["position"] = pd.to_numeric(df["position"], errors="coerce")

pivot_df = df.groupby("unique_key").agg(
    position_count=("position", "count"),
    max_position=("position", "max")
).reset_index()

pivot_df["position_validation_status"] = pivot_df.apply(
    lambda row: (
        "LESS_THAN_60_OK" if (row["position_count"] < 60 and row["max_position"] == row["position_count"])
        else "LESS_THAN_60_INVALID" if (row["position_count"] < 60)
        else "HAS_60"
    ),
    axis=1
)



df = df.merge(pivot_df, on="unique_key", how="left")

# ================= EVIDENCE URL CHECK =================
def check_evidence_url(url):
    if is_na(url):
        return "MISSING"
    try:
        r = requests.head(url, timeout=5, allow_redirects=True)
        if r.status_code == 200:
            return "OK"
        if r.status_code == 404:
            return "NOT_FOUND"
        return f"ERROR_{r.status_code}"
    except:
        return "ERROR"

unique_urls = df["evidence_url"].dropna().unique()
url_status_map = {}

with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(check_evidence_url, url): url for url in unique_urls}
    
    completed = 0
    total = len(futures)

    for f in as_completed(futures):
        completed += 1
        if completed % 100 == 0:
            print(f"Checked {completed}/{total} URLs", flush=True)

        url_status_map[futures[f]] = f.result()

df["evidence_url_validation"] = df["evidence_url"].map(url_status_map)

print("Starting column checks...", flush=True)


# ================= COLUMN INPUT VALIDATION =================
# def apply_check(col, ip_key, is_url=False):
#     out = df[col].apply(lambda x: exists_in_ip_verbose(x, ip_sets[ip_key], is_url=is_url))
#     df[f"{col}_ip_check"], df[f"{col}_missing"], df[f"{col}_closest"] = zip(*out)

def apply_check(col, ip_key, is_url=False):
    valid_set = ip_sets[ip_key]

    # ---------- STEP 1: FAST PASS/FAIL (vectorized) ----------
    if is_url:
        series = df[col].apply(normalize_url_for_compare)
    else:
        series = df[col].astype(str).str.strip()

    mask_fail = ~series.isin(valid_set)

    df[f"{col}_ip_check"] = np.where(mask_fail, "FAIL", "PASS")
    df[f"{col}_missing"] = ""
    df[f"{col}_closest"] = ""

    # ---------- STEP 2: difflib ONLY for FAILED rows ----------
    failed_idx = df.index[mask_fail]

    for i in failed_idx:
        val = series.iloc[i]

        df.at[i, f"{col}_missing"] = val

        if USE_DIFFLIB:
            close = difflib.get_close_matches(val, valid_set, n=1, cutoff=0.6)
            df.at[i, f"{col}_closest"] = close[0] if close else ""


apply_check("keywords", "keywords")
apply_check("purl", "purl", is_url=True)
apply_check("top_category_lvmh", "top_category_lvmh")
apply_check("category_lvmh", "category_lvmh")
apply_check("sub_category_lvmh", "sub_category_lvmh")

# ================= DATE / LISTING =================
today = datetime.today().strftime("%Y-%m-%d")
df["date_check"] = df["date"].apply(lambda x: "PASS" if str(x).startswith(today) else "FAIL")
df["product_page_url_check"] = df["product_page_url"].apply(lambda x: "FAIL" if is_na(x) else "PASS")
df["listing_type_check"] = df["listing_type"].apply(lambda x: "PASS" if str(x).strip() in {"Organic", "Sponsored"} else "FAIL")

# ================= FINAL FAILURE REASON =================
FAILURE_MESSAGE_MAP = {
    "productid_check": "productid is not in required format for example : B0719S9TPB_48_60",
    "scope_check": "scope should match as per snowflake database",
    "rname_check": "rname should match as per snowflake database",
    "country_check": "country should match as per snowflake database",
    "brand_check": "brand should not contain n/a values",
    "pname_check": "pname should not contain n/a values",
    "keywords_ip_check": "keywords need to have all values which are available in IP",
    "purl_ip_check": "purl should contain all urls which are in IP",
    "top_category_lvmh_ip_check": "top_category_lvmh mismatch with IP",
    "category_lvmh_ip_check": "category_lvmh mismatch with IP",
    "sub_category_lvmh_ip_check": "sub_category_lvmh mismatch with IP",
    "date_check": "date should be current date",
    "product_page_url_check": "product_page_url should not be n/a",
    "listing_type_check": "listing_type should be Organic or Sponsored",
    "position_validation_status": "position count less than 60",
    "Total_results= position": "Neither total_results is  equal to position nor 120"
}

CHECK_COLUMNS = list(FAILURE_MESSAGE_MAP.keys())

def build_failure_reason(row):
    reasons = []
    for col in CHECK_COLUMNS:
        if col in row and str(row[col]).upper() in {"FAIL","LESS_THAN_60"}:
            reasons.append(FAILURE_MESSAGE_MAP[col])
    if row.get("evidence_url_validation") in {"NOT_FOUND","ERROR","MISSING"}:
        reasons.append("evidence_url not valid or mismatch with page")
    return " | ".join(dict.fromkeys(reasons))

# ================= FAST + DETAILED FAILURE REASON =================

reasons = []

for col, msg in FAILURE_MESSAGE_MAP.items():
    if col in df.columns:
        reasons.append(np.where(df[col].eq("FAIL"), msg, ""))

# evidence url check
if "evidence_url_validation" in df.columns:
    reasons.append(
        np.where(
            df["evidence_url_validation"].isin(["NOT_FOUND", "ERROR", "MISSING"]),
            "evidence_url not valid or mismatch with page",
            ""
        )
    )

reason_df = pd.DataFrame(reasons).T

df["failure_reason"] = reason_df.apply(
    lambda x: " | ".join(filter(None, x)),
    axis=1
)

df["overall_status"] = np.where(df["failure_reason"] == "", "PASS", "FAIL")


# Move failure_reason to end
cols = [c for c in df.columns if c != "failure_reason"] + ["failure_reason"]
df = df[cols]
print("Finished column checks", flush=True)


# ================= FIND MISSING URLS (IN IP BUT NOT IN OP) =================

# Build OP url set per scope
op_scope_to_urls = {}

for _, r in df.iterrows():
    scope = str(r.get("scope")).strip()
    url = str(r.get("purl")).strip()

    if url and str(url).strip().lower() not in {"", "n/a", "na", "null", "nan"}:
        op_scope_to_urls.setdefault(scope, set()).add(url)

missing_rows = []

for scope, ip_urls in ip_scope_to_urls.items():
    op_urls = op_scope_to_urls.get(scope, set())

    missing_urls = ip_urls - op_urls

    for u in missing_urls:
        missing_rows.append({
            "scope": scope,
            "missing_url": u
        })

missing_urls_df = pd.DataFrame(missing_rows)

print(f"🔍 Total Missing URLs Found: {len(missing_urls_df)}")

# ================= OUTPUT =================
# ================= OUTPUT =================
print("Starting Excel write...", flush=True)

output_file = OUTPUT_FILE

# -------- CSV (fast full dump) --------
df.to_csv(output_file.with_suffix(".csv"), index=False)

# -------- Excel (FAST writer) --------
with pd.ExcelWriter(
    output_file,
    engine="xlsxwriter",
    engine_kwargs={"options": {"strings_to_urls": False}}   # faster + avoids auto hyperlink parsing
) as writer:

    # Main comparison sheet
    df.to_excel(writer, sheet_name="PL_Data", index=False)

    # Pivot sheet
    pivot_df.to_excel(writer, sheet_name="Pivot_Position_Check", index=False)

    # Missing URLs
    missing_urls_df.to_excel(
        writer,
        sheet_name="Missing_URLs",
        index=False
    )

    # Extra columns
    extra_columns_df.to_excel(
        writer,
        sheet_name="Extra_Columns",
        index=False
    )

print("Finished Excel write", flush=True)
print(f"✅ OUTPUT GENERATED: {output_file}")
print("=== PL CONSO AUTOMATION COMPLETED ===")
