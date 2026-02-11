import pandas as pd
import numpy as np
import re
from pathlib import Path
import sys
from datetime import datetime
import os
import time
from openpyxl import load_workbook
import json

print("=== PDP CHECKLIST STARTED ===", flush=True)

# ================= ARGUMENTS =================
if len(sys.argv) != 5:
    print("Usage: python pdp_check.py <CRAWL_OUTPUT> <CRAWL_INPUT> <MASTER_FILE> <OUTPUT_FILE>")
    sys.exit(1)

OP_FILE = Path(sys.argv[1])   # crawl output to be checked
IP_FILE = Path(sys.argv[2])   # crawl input (consolidated)
MASTER_FILE = Path(sys.argv[3])
OUTPUT_FILE = Path(sys.argv[4])

DISPLAY_NAMES = {
    str(OP_FILE): os.environ.get("PDP_OP_NAME", ""),
    str(IP_FILE): os.environ.get("PDP_IP_NAME", ""),
    str(MASTER_FILE): os.environ.get("PDP_MASTER_NAME", ""),
}

def display_name(fp: Path) -> str:
    return DISPLAY_NAMES.get(str(fp), "") or fp.name

print(f"📄 Output File : {display_name(OP_FILE)}")
print(f"📄 Input File  : {display_name(IP_FILE)}")
print(f"📄 Master File : {display_name(MASTER_FILE)}")
print(f"📄 Result File : {OUTPUT_FILE}")

for fp, label in [(OP_FILE, "Output"), (IP_FILE, "Input"), (MASTER_FILE, "Master")]:
    if not fp.exists():
        print(f"❌ {label} file not found: {fp}")
        sys.exit(1)

def is_excel_display(fp: Path) -> bool:
    return display_name(fp).lower().endswith((".xlsx", ".xls"))

def is_excel_file(fp: Path) -> bool:
    try:
        with open(fp, "rb") as f:
            sig = f.read(8)
        # XLSX is a zip archive (PK\x03\x04). XLS (OLE) starts with D0 CF 11 E0 A1 B1 1A E1
        return sig.startswith(b"PK\x03\x04") or sig.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1")
    except Exception:
        return False

def excel_compatible_path(fp: Path) -> Path | None:
    """
    Ensure a path with an Excel-compatible extension if the content is Excel.
    Returns a temp path if created, or the original path if already compatible.
    """
    excel_exts = {".xlsx", ".xlsm", ".xltx", ".xltm", ".xls"}
    if fp.suffix.lower() in excel_exts:
        return fp
    if not is_excel_file(fp):
        return None
    tmp_path = fp.with_suffix(".xlsx")
    try:
        with open(fp, "rb") as src, open(tmp_path, "wb") as dst:
            dst.write(src.read())
        return tmp_path
    except Exception:
        return None

# ================= FILE HELPERS =================
def read_file(fp: Path) -> pd.DataFrame:
    display = display_name(fp)
    display_lower = display.lower()
    suffix = fp.suffix.lower()

    # If original filename indicates Excel, prefer read_excel only if content is Excel
    if display_lower.endswith((".xlsx", ".xls")):
        excel_path = excel_compatible_path(fp)
        if excel_path is None:
            pass
        else:
            # If this is the input file, load only needed columns to speed up
            if fp == IP_FILE:
                print(f"⏳ Loading Excel (header only): {display}", flush=True)
                header_df = pd.read_excel(excel_path, nrows=0)
                header_cols = [c.strip().lower() for c in header_df.columns]
                scope_col = "scope" if "scope" in header_cols else ("scope_name" if "scope_name" in header_cols else None)
                rname_col = "rname" if "rname" in header_cols else ("domain_input" if "domain_input" in header_cols else None)

                if scope_col and rname_col:
                    print(f"⏳ Loading Excel (usecols={scope_col},{rname_col}): {display}", flush=True)
                    return pd.read_excel(excel_path, dtype=str, keep_default_na=False, usecols=[scope_col, rname_col])

            print(f"⏳ Loading Excel: {display}", flush=True)
            return pd.read_excel(excel_path, dtype=str, keep_default_na=False)

    if suffix == ".tsv" or display_lower.endswith(".tsv"):
        try:
            print(f"⏳ Loading TSV: {display}", flush=True)
            return pd.read_csv(fp, sep="\t", dtype=str, encoding="utf-8", keep_default_na=False)
        except UnicodeDecodeError:
            print(f"⚠️ WARNING: {display} is not UTF-8. Reading as latin1 (may corrupt data).")
            try:
                print(f"⏳ Loading TSV (latin1): {display}", flush=True)
                return pd.read_csv(fp, sep="\t", dtype=str, encoding="latin1", keep_default_na=False)
            except pd.errors.ParserError:
                print(f"⚠️ WARNING: {display} has malformed lines. Retrying with python engine and skipping bad lines.")
                return pd.read_csv(
                    fp,
                    sep="\t",
                    dtype=str,
                    encoding="latin1",
                    keep_default_na=False,
                    engine="python",
                    on_bad_lines="skip"
                )
        except pd.errors.ParserError:
            print(f"⚠️ WARNING: {display} has malformed lines. Retrying with python engine and skipping bad lines.")
            return pd.read_csv(
                fp,
                sep="\t",
                dtype=str,
                encoding="utf-8",
                keep_default_na=False,
                engine="python",
                on_bad_lines="skip"
            )
    if suffix == ".csv" or display_lower.endswith(".csv"):
        print(f"⏳ Loading CSV: {display}", flush=True)
        return pd.read_csv(fp, dtype=str, keep_default_na=False)
    print(f"⏳ Loading file: {display}", flush=True)
    return pd.read_excel(fp, dtype=str, keep_default_na=False)

def compute_ip_counts_excel(fp: Path, scopes_in_output: set | None):
    display = display_name(fp)
    print(f"⏳ Streaming Excel for counts: {display}", flush=True)
    wb = load_workbook(fp, read_only=True, data_only=True)
    ws = wb.active

    header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
    header = [str(v).strip().lower() if v is not None else "" for v in header_row]

    scope_idx = header.index("scope") if "scope" in header else (header.index("scope_name") if "scope_name" in header else None)
    rname_idx = header.index("rname") if "rname" in header else (header.index("domain_input") if "domain_input" in header else None)

    if scope_idx is None or rname_idx is None:
        wb.close()
        return None, None, None, None

    counts = {}
    total_rows = 0
    kept_rows = 0

    max_col = max(scope_idx, rname_idx) + 1
    for row in ws.iter_rows(min_row=2, max_col=max_col, values_only=True):
        total_rows += 1
        scope_val = row[scope_idx]
        if scope_val is None:
            continue
        scope_s = str(scope_val).strip()
        if scopes_in_output and scope_s not in scopes_in_output:
            continue
        rname_val = row[rname_idx]
        rname_s = "" if rname_val is None else str(rname_val).strip()
        counts[(scope_s, rname_s)] = counts.get((scope_s, rname_s), 0) + 1
        kept_rows += 1

    wb.close()
    return counts, total_rows, kept_rows, (header[scope_idx], header[rname_idx])

def compute_ip_counts_pandas_excel(fp: Path, scopes_in_output: set | None):
    display = display_name(fp)
    print(f"⏳ Loading Excel (pandas usecols): {display}", flush=True)
    header_df = pd.read_excel(fp, nrows=0)
    header_cols = [c.strip().lower() for c in header_df.columns]
    scope_col = "scope" if "scope" in header_cols else ("scope_name" if "scope_name" in header_cols else None)
    rname_col = "rname" if "rname" in header_cols else ("domain_input" if "domain_input" in header_cols else None)

    if scope_col is None or rname_col is None:
        return None, None, None, None

    df_ip = pd.read_excel(fp, dtype=str, keep_default_na=False, usecols=[scope_col, rname_col])
    total_rows = len(df_ip)
    df_ip.columns = [c.strip().lower() for c in df_ip.columns]
    if "scope" not in df_ip.columns and "scope_name" in df_ip.columns:
        df_ip = df_ip.rename(columns={"scope_name": "scope"})
    if "rname" not in df_ip.columns and "domain_input" in df_ip.columns:
        df_ip = df_ip.rename(columns={"domain_input": "rname"})

    if scopes_in_output:
        df_ip = df_ip[df_ip["scope"].isin(scopes_in_output)]
    kept_rows = len(df_ip)
    ip_key = list(zip(df_ip["scope"].astype(str).str.strip(), df_ip["rname"].astype(str).str.strip()))
    counts = pd.Series(ip_key).value_counts().to_dict()
    return counts, total_rows, kept_rows, (scope_col, rname_col)

def encode_counts(counts: dict) -> dict:
    return {f"{k[0]}|||{k[1]}": v for k, v in counts.items()}

def decode_counts(data: dict) -> dict:
    counts = {}
    for k, v in data.items():
        if "|||" in k:
            scope, rname = k.split("|||", 1)
        else:
            scope, rname = k, ""
        counts[(scope, rname)] = int(v)
    return counts

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
t0 = time.perf_counter()
df = read_file(OP_FILE)
print(f"✅ Loaded OP in {time.perf_counter() - t0:.2f}s | rows={len(df)}", flush=True)

t0 = time.perf_counter()
master_df = read_file(MASTER_FILE)
print(f"✅ Loaded MASTER in {time.perf_counter() - t0:.2f}s | rows={len(master_df)}", flush=True)

# Normalize column names
df.columns = [c.strip().lower() for c in df.columns]
master_df.columns = [c.strip().lower() for c in master_df.columns]

# ================= REQUIRED BASE COLUMNS =================
required_cols = ["scope", "rname", "country"]
for col in required_cols:
    if col not in df.columns:
        print(f"❌ Output file missing '{col}' column")
        sys.exit(1)

scopes_in_output = set(df["scope"].dropna().astype(str).str.strip())
print("🔎 Scopes found in output:", scopes_in_output)

# ================= LOAD INPUT (FAST PATH FOR EXCEL) =================
ip_df = None
ip_counts_precomputed = None
cache_loaded = False
cache_path = os.getenv("PDP_IP_COUNTS_CACHE", "").strip()
cache_write = os.getenv("PDP_IP_COUNTS_CACHE_WRITE", "").strip()
want_full_cache = bool(cache_write)
if cache_path and os.path.exists(cache_path):
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        ip_counts_precomputed = decode_counts(payload.get("counts", {}))
        cache_loaded = True
        print("✅ Loaded input counts cache", flush=True)
    except Exception as e:
        print(f"⚠️ Failed to load input counts cache. {str(e)}", flush=True)

if is_excel_display(IP_FILE):
    excel_path = excel_compatible_path(IP_FILE)
    if excel_path is not None:
        t0 = time.perf_counter()
        use_pandas_excel = os.getenv("PDP_EXCEL_PANDAS", "0").strip() == "1"
        scopes_for_count = None if (want_full_cache and not cache_loaded) else scopes_in_output
        if use_pandas_excel and not cache_loaded:
            ip_counts_precomputed, total_rows, kept_rows, ip_cols = compute_ip_counts_pandas_excel(excel_path, scopes_for_count)
            if ip_counts_precomputed is None:
                print("⚠️ Pandas Excel load failed to find scope/rname columns. Falling back to streaming.", flush=True)
                ip_counts_precomputed, total_rows, kept_rows, ip_cols = compute_ip_counts_excel(excel_path, scopes_for_count)
        elif not cache_loaded:
            ip_counts_precomputed, total_rows, kept_rows, ip_cols = compute_ip_counts_excel(excel_path, scopes_for_count)
    else:
        ip_counts_precomputed, total_rows, kept_rows, ip_cols = None, None, None, None
    if ip_counts_precomputed is None:
        print("⚠️ Excel streaming failed to find scope/rname columns. Falling back to full load.", flush=True)
    elif not cache_loaded:
        print(f"✅ Streamed IP in {time.perf_counter() - t0:.2f}s | rows={total_rows} | kept={kept_rows} | cols={ip_cols}", flush=True)

if ip_counts_precomputed is None:
    t0 = time.perf_counter()
    ip_df = read_file(IP_FILE)
    print(f"✅ Loaded IP in {time.perf_counter() - t0:.2f}s | rows={len(ip_df)}", flush=True)

    # Normalize column names
    ip_df.columns = [c.strip().lower() for c in ip_df.columns]

    # Normalize common alternate column names
    if "scope" not in ip_df.columns and "scope_name" in ip_df.columns:
        print("ℹ️ Renaming scope_name -> scope in INPUT file")
        ip_df = ip_df.rename(columns={"scope_name": "scope"})

    # Filter input by scopes found in output
    if "scope" in ip_df.columns:
        ip_df = ip_df[ip_df["scope"].isin(scopes_in_output)]
    print(f"✅ IP rows after scope filter: {len(ip_df)}", flush=True)

print(f"✅ Files loaded | OP Rows: {len(df)} | IP Rows: {len(ip_df) if ip_df is not None else 'streamed'} | Master Rows: {len(master_df)}")

# Write cache if requested and computed
if cache_write and ip_counts_precomputed is not None and not cache_loaded:
    try:
        payload = {
            "counts": encode_counts(ip_counts_precomputed)
        }
        with open(cache_write, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        print("✅ Input counts cache written", flush=True)
    except Exception as e:
        print(f"⚠️ Failed to write input counts cache. {str(e)}", flush=True)

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

# ================= UNIQUE KEY =================
base_id = df.get("base_id", pd.Series([""] * len(df))).astype(str).str.strip()
rname_key = df["rname"].astype(str).str.strip()
country_key = df["country"].astype(str).str.strip()

df["unique_key"] = rname_key + base_id

# Hermes special case: rname + base_id + country
mask_hermes = rname_key.str.upper().str.contains("HERMES_EUROPE", na=False)
df.loc[mask_hermes, "unique_key"] = rname_key[mask_hermes] + base_id[mask_hermes] + country_key[mask_hermes]

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

if ip_counts_precomputed is not None:
    ip_counts = ip_counts_precomputed
else:
    ip_scope_col = "scope" if (ip_df is not None and "scope" in ip_df.columns) else None
    ip_rname_col = None
    if ip_df is not None and "rname" in ip_df.columns:
        ip_rname_col = "rname"
    elif ip_df is not None and "domain_input" in ip_df.columns:
        ip_rname_col = "domain_input"

    if ip_scope_col and ip_rname_col:
        ip_key = list(zip(
            ip_df[ip_scope_col].astype(str).str.strip(),
            ip_df[ip_rname_col].astype(str).str.strip()
        ))
        ip_counts = pd.Series(ip_key).value_counts().to_dict()
    else:
        ip_counts = {}

expected_counts = [ip_counts.get(k, 0) for k in row_key]
actual_counts = [op_counts.get(k, 0) for k in row_key]

df["row_count_input"] = expected_counts
df["row_count_output"] = actual_counts
df["row_count_check"] = np.where(df["row_count_input"] == df["row_count_output"], "PASS", "FAIL")

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
