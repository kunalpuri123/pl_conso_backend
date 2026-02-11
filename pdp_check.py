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

def compute_ip_counts_excel(fp: Path, scopes_in_output: set | None, collect_input_map: bool = False):
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
    input_map = {}
    total_rows = 0
    kept_rows = 0

    # Try to capture optional fields for input map
    base_id_idx = header.index("base_id") if "base_id" in header else None
    country_idx = header.index("country") if "country" in header else None
    pname_idx = header.index("pname") if "pname" in header else None
    skuvar_idx = header.index("skuvarient") if "skuvarient" in header else None

    max_col = max(i for i in [scope_idx, rname_idx, base_id_idx, country_idx, pname_idx, skuvar_idx] if i is not None) + 1
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
        # build input map if columns exist
        if collect_input_map and base_id_idx is not None and country_idx is not None and (pname_idx is not None or skuvar_idx is not None):
            base_id = "" if row[base_id_idx] is None else str(row[base_id_idx]).strip()
            country = "" if row[country_idx] is None else str(row[country_idx]).strip()
            key = f"{rname_s}{base_id}{country}"
            pname_val = "" if pname_idx is None or row[pname_idx] is None else str(row[pname_idx]).strip()
            skuvar_val = "" if skuvar_idx is None or row[skuvar_idx] is None else str(row[skuvar_idx]).strip()
            if key not in input_map:
                input_map[key] = {"pname": pname_val, "skuvarient": skuvar_val}
        kept_rows += 1

    wb.close()
    return counts, total_rows, kept_rows, (header[scope_idx], header[rname_idx]), input_map

def compute_ip_counts_pandas_excel(fp: Path, scopes_in_output: set | None, collect_input_map: bool = False):
    display = display_name(fp)
    print(f"⏳ Loading Excel (pandas usecols): {display}", flush=True)
    header_df = pd.read_excel(fp, nrows=0)
    header_cols = [c.strip().lower() for c in header_df.columns]
    scope_col = "scope" if "scope" in header_cols else ("scope_name" if "scope_name" in header_cols else None)
    rname_col = "rname" if "rname" in header_cols else ("domain_input" if "domain_input" in header_cols else None)

    if scope_col is None or rname_col is None:
        return None, None, None, None, None

    # try to pull optional fields for input map
    base_id_col = "base_id" if "base_id" in header_cols else None
    country_col = "country" if "country" in header_cols else None
    pname_col = "pname" if "pname" in header_cols else None
    skuvar_col = "skuvarient" if "skuvarient" in header_cols else None
    extra_cols = [base_id_col, country_col, pname_col, skuvar_col] if collect_input_map else []
    usecols = [c for c in [scope_col, rname_col, *extra_cols] if c]

    df_ip = pd.read_excel(fp, dtype=str, keep_default_na=False, usecols=usecols)
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

    input_map = {}
    if collect_input_map and {"base_id", "country"}.issubset(df_ip.columns):
        rname_s = df_ip["rname"].astype(str).str.strip()
        base_s = df_ip["base_id"].astype(str).str.strip()
        country_s = df_ip["country"].astype(str).str.strip()
        pname_s = df_ip["pname"].astype(str).str.strip() if "pname" in df_ip.columns else pd.Series([""] * len(df_ip))
        skuvar_s = df_ip["skuvarient"].astype(str).str.strip() if "skuvarient" in df_ip.columns else pd.Series([""] * len(df_ip))
        for rn, b, c, pn, sv in zip(rname_s, base_s, country_s, pname_s, skuvar_s):
            key = f"{rn}{b}{c}"
            if key not in input_map:
                input_map[key] = {"pname": pn, "skuvarient": sv}

    return counts, total_rows, kept_rows, (scope_col, rname_col), input_map

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

def encode_input_map(input_map: dict) -> dict:
    return input_map

def decode_input_map(data: dict) -> dict:
    return data or {}

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
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
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

def scope_key_from_value(val: str) -> str:
    s = str(val or "").strip()
    if not s:
        return ""
    return s.split("_")[0].strip().lower()

# Benefit-specific column aliases (apply to all checks)
benefit_scope = any(scope_key_from_value(s) == "benefit" for s in df.get("scope", pd.Series([""])).astype(str).str.strip())
if benefit_scope:
    if "rname" not in df.columns and "site_name" in df.columns:
        df["rname"] = df["site_name"]
    if "rating" not in df.columns and "average_rating" in df.columns:
        df["rating"] = df["average_rating"]
    if "country" not in df.columns and "region_site" in df.columns:
        df["country"] = df["region_site"]

# ================= REQUIRED BASE COLUMNS =================
required_cols = ["scope", "rname", "country"]
for col in required_cols:
    if col not in df.columns:
        print(f"❌ Output file missing '{col}' column")
        sys.exit(1)

base_columns = list(df.columns)

scopes_in_output = set(df["scope"].dropna().astype(str).str.strip())
print("🔎 Scopes found in output:", scopes_in_output)

# ================= LOAD INPUT (FAST PATH FOR EXCEL) =================
ip_df = None
ip_counts_precomputed = None
ip_input_map = {}
cache_loaded = False
cache_path = os.getenv("PDP_IP_COUNTS_CACHE", "").strip()
cache_write = os.getenv("PDP_IP_COUNTS_CACHE_WRITE", "").strip()
want_full_cache = bool(cache_write)
if cache_path and os.path.exists(cache_path):
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        ip_counts_precomputed = decode_counts(payload.get("counts", {}))
        ip_input_map = decode_input_map(payload.get("input_map", {}))
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
        collect_input_map = scope_key_main in {"mfk", "hermes"}
        if use_pandas_excel and not cache_loaded:
            ip_counts_precomputed, total_rows, kept_rows, ip_cols, ip_input_map = compute_ip_counts_pandas_excel(
                excel_path, scopes_for_count, collect_input_map=collect_input_map
            )
            if ip_counts_precomputed is None:
                print("⚠️ Pandas Excel load failed to find scope/rname columns. Falling back to streaming.", flush=True)
                ip_counts_precomputed, total_rows, kept_rows, ip_cols, ip_input_map = compute_ip_counts_excel(
                    excel_path, scopes_for_count, collect_input_map=collect_input_map
                )
        elif not cache_loaded:
            ip_counts_precomputed, total_rows, kept_rows, ip_cols, ip_input_map = compute_ip_counts_excel(
                excel_path, scopes_for_count, collect_input_map=collect_input_map
            )
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
            "counts": encode_counts(ip_counts_precomputed),
            "input_map": encode_input_map(ip_input_map)
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

# If input map was not computed, try to derive from ip_df (non-excel path)
if not ip_input_map and ip_df is not None and scope_key_main in {"mfk", "hermes"}:
    if {"rname","base_id","country"}.issubset(ip_df.columns):
        rname_s = ip_df["rname"].astype(str).str.strip()
        base_s = ip_df["base_id"].astype(str).str.strip()
        country_s = ip_df["country"].astype(str).str.strip()
        pname_s = ip_df["pname"].astype(str).str.strip() if "pname" in ip_df.columns else pd.Series([""] * len(ip_df))
        skuvar_s = ip_df["skuvarient"].astype(str).str.strip() if "skuvarient" in ip_df.columns else pd.Series([""] * len(ip_df))
        for rn, b, c, pn, sv in zip(rname_s, base_s, country_s, pname_s, skuvar_s):
            key = f"{rn}{b}{c}"
            if key not in ip_input_map:
                ip_input_map[key] = {"pname": pn, "skuvarient": sv}

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

def is_na_or_not_available_series(s: pd.Series) -> pd.Series:
    return series_is_na(s) | is_not_available_series(s)

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

# ================= NOT AVAILABLE VALUE CHECK =================
NOT_AVAILABLE_REQUIRED_BY_SCOPE = {
    "hermes": ["top_category","category","sub_category","RName","country","SKUVARIENT","PName","Brand","Date","scope","base_id"],
    "coty": ["top_category","category","sub_category","RName","country","SKUVARIENT","Date","scope","base_id","url"],
    "mfk": ["top_category","category","sub_category","RName","country","SKUVARIENT","PName","Brand","Date","scope","base_id"],
    "pcd": ["top_category","category","sub_category","RName","country","SKUVARIENT","PName","Brand","Date","scope","base_id","url"],
}
ALLOWED_NA_REQUIRED_BY_SCOPE = {
    "pcd": {"url"},
}

scope_name = df.get("scope", pd.Series([""])).astype(str).str.strip().iloc[0] if len(df) else ""
scope_key = scope_key_from_value(scope_name)
required_na_cols = NOT_AVAILABLE_REQUIRED_BY_SCOPE.get(scope_key)
df["not_available_values_check"] = "PASS"

if required_na_cols:
    na_mask = stock_status.str.strip().str.lower() == "not available"
    if na_mask.any():
        col_map = {c.lower(): c for c in base_columns}
        required_na_lower = [c.lower() for c in required_na_cols]
        allowed_na = {c.lower() for c in ALLOWED_NA_REQUIRED_BY_SCOPE.get(scope_key, set())}

        required_actual = []
        missing_required = []
        for req_l in required_na_lower:
            actual = col_map.get(req_l)
            if actual:
                required_actual.append(actual)
            else:
                missing_required.append(req_l)

        fail_mask = pd.Series(False, index=df.index)
        if missing_required:
            fail_mask |= na_mask

        # Required columns should NOT be not available (except allowed)
        for col in required_actual:
            if col.lower() in allowed_na:
                continue
            fail_mask |= na_mask & is_na_or_not_available_series(df[col])

        # Non-required columns should be not available
        other_cols = [c for c in base_columns if c.lower() not in set(required_na_lower)]
        for col in other_cols:
            fail_mask |= na_mask & ~is_na_or_not_available_series(df[col])

        df.loc[fail_mask, "not_available_values_check"] = "FAIL"

# ================= RATING CHECK =================
rating = df.get("rating", pd.Series([""] * len(df))).astype(str).str.strip()
review = df.get("review", pd.Series([""] * len(df))).astype(str).str.strip()

df["rating_check"] = "PASS"

rating_is_na = rating.apply(is_na_text) | rating.apply(is_not_available)
review_is_na = review.apply(is_na_text) | review.apply(is_not_available)
# normalize decimal comma and remove stray spaces
rating_norm = rating.str.replace(",", ".", regex=False).str.strip()
rating_norm = rating_norm.replace({"": np.nan})
rating_numeric_ok = rating_norm.str.match(r"^[0-9]+(\.[0-9]+)?$", na=False)

rating_val = pd.to_numeric(rating_norm.where(rating_numeric_ok, np.nan), errors="coerce")
rating_in_range = (rating_val >= 0) & (rating_val <= 5)

review_val = pd.to_numeric(review, errors="coerce").fillna(0)

# If stock status is Not available, allow text rating
mask_stock_na = stock_status == "Not available"

# Exceptional case: rating and review both Not available -> PASS
mask_rating_review_na = rating.apply(is_not_available) & review.apply(is_not_available)
df.loc[mask_rating_review_na, "rating_check"] = "PASS"

# For non-Not-available stock, rating must be numeric 0-5 and no spaces
df.loc[~mask_stock_na & (~rating_numeric_ok | ~rating_in_range) & ~mask_rating_review_na, "rating_check"] = "FAIL"

# If review > 0, rating must be present
df.loc[(review_val > 0) & rating_is_na & ~mask_rating_review_na, "rating_check"] = "FAIL"

# For MFK and Hermes: In Stock / Out of Stock must not have Not available in rating or review
df["rating_review_stock_check"] = "PASS"
scope_key_rr = scope_key_from_value(df.get("scope", pd.Series([""])).astype(str).str.strip().iloc[0] if len(df) else "")
if scope_key_rr in {"mfk", "hermes"}:
    mask_stock_in_out = stock_status.isin(["In Stock", "Out of Stock"])
    mask_rating_na = rating.apply(is_not_available)
    mask_review_na = review.apply(is_not_available)
df.loc[mask_stock_in_out & (mask_rating_na | mask_review_na), "rating_review_stock_check"] = "FAIL"

# For MFK and Hermes: Not available SKUs must have PName and SKUVARIENT from input (not Not available)
df["na_sku_input_check"] = "PASS"
scope_key_rr = scope_key_from_value(df.get("scope", pd.Series([""])).astype(str).str.strip().iloc[0] if len(df) else "")
if scope_key_rr in {"mfk", "hermes"} and ip_input_map:
    mask_na_sku = stock_status.str.strip().str.lower() == "not available"
    if mask_na_sku.any():
        pname_col = None
        skuvar_col = None
        for c in df.columns:
            if c.lower() == "pname":
                pname_col = c
            elif c.lower() == "skuvarient":
                skuvar_col = c
        if pname_col is None or skuvar_col is None:
            df.loc[mask_na_sku, "na_sku_input_check"] = "FAIL"
        else:
            out_rname = df["rname"].astype(str).str.strip()
            out_base = df.get("base_id", pd.Series([""] * len(df))).astype(str).str.strip()
            out_country = df.get("country", pd.Series([""] * len(df))).astype(str).str.strip()
            out_pname = df[pname_col].astype(str).str.strip()
            out_skuvar = df[skuvar_col].astype(str).str.strip()
            for i in df[mask_na_sku].index:
                key = f"{out_rname[i]}{out_base[i]}{out_country[i]}"
                ref = ip_input_map.get(key)
                if not ref:
                    df.at[i, "na_sku_input_check"] = "FAIL"
                    continue
                if is_na_text(out_pname[i]) or is_not_available(out_pname[i]) or is_na_text(out_skuvar[i]) or is_not_available(out_skuvar[i]):
                    df.at[i, "na_sku_input_check"] = "FAIL"
                    continue
                if ref.get("pname", "") and out_pname[i] != ref.get("pname", ""):
                    df.at[i, "na_sku_input_check"] = "FAIL"
                    continue
                if ref.get("skuvarient", "") and out_skuvar[i] != ref.get("skuvarient", ""):
                    df.at[i, "na_sku_input_check"] = "FAIL"

# Check rating normalization for MFK and Hermes only (no mutation)
scope_key_rating = scope_key_from_value(df.get("scope", pd.Series([""])).astype(str).str.strip().iloc[0] if len(df) else "")
df["rating_normalization_check"] = "PASS"
if scope_key_rating in {"mfk", "hermes"}:
    mask_rating_valid = rating_numeric_ok & rating_in_range
    mask_rating_zero = mask_rating_valid & (rating_val == 0)
    mask_rating_int = mask_rating_valid & (rating_val % 1 == 0) & ~mask_rating_zero
    # Integer > 0 must be written as X.0 (e.g., 2.0). Zero must be "0"
    normalized_int_ok = rating_norm.str.match(r"^[0-9]+\\.0+$", na=False)
    normalized_zero_ok = rating_norm.eq("0")
    fail_norm = (mask_rating_int & ~normalized_int_ok) | (mask_rating_zero & ~normalized_zero_ok)
    df.loc[fail_norm, "rating_normalization_check"] = "FAIL"

# ================= KEYWORDS BLANK CHECK (scope-specific) =================
KEYWORDS_BLANK_SCOPES = {"mfk", "hermes"}
df["keywords_blank_check"] = "PASS"
scope_key = scope_key_from_value(df.get("scope", pd.Series([""])).astype(str).str.strip().iloc[0] if len(df) else "")
if scope_key in KEYWORDS_BLANK_SCOPES:
    # find keywords column case-insensitively
    keywords_col = None
    for c in df.columns:
        if c.lower() == "keywords":
            keywords_col = c
            break
    if keywords_col is None:
        df["keywords_blank_check"] = "FAIL"
    else:
        kw_vals = df[keywords_col].astype(str).str.strip()
        non_blank = ~kw_vals.apply(is_na_text)
        df.loc[non_blank, "keywords_blank_check"] = "FAIL"

# ================= FAILURE REASON =================
REQUIRED_COLUMNS_BY_SCOPE = {
    # MFK and PCD scopes
    "MFK": [
        "top_category","category","sub_category","pid","key","RName","country","MPN","SKUVARIENT","SKUL","PName",
        "Division","Category","Department","Class","SubClass","Brand","RegularPrice","FinalPrice","PriceRange",
        "Availability","Rating","Review","Promotion","Position","Channel","currency","ProductImage","KeyWords",
        "day","month","year","Date","scope","Product_Description","base_id","region_input","currency_input",
        "stock_status","item_status","markdown_price","url","category_path","add_to_cart","multiple_url",
        "large_image","small_image","normalized_Brand","spid"
    ],
    "PCD": [
        "top_category","category","sub_category","pid","key","RName","country","MPN","SKUVARIENT","SKUL","PName",
        "Division","Category","Department","Class","SubClass","Brand","RegularPrice","FinalPrice","PriceRange",
        "Availability","Rating","Review","Promotion","Position","Channel","currency","ProductImage","KeyWords",
        "day","month","year","Date","scope","Product_Description","base_id","region_input","currency_input",
        "stock_status","item_status","markdown_price","url","category_path","add_to_cart","multiple_url",
        "large_image","small_image","normalized_Brand","spid"
    ],
    "Benefit": [
        "site_name","category_path","category_path_url","division","category","department","class","subclass",
        "product_id","product_name","product_description","product_dimensions","product_weight","product_material",
        "url","product_image","regular_price","regular_price_range","shipping_price","markdown_price",
        "disounted_price","final_price","item_status","item_level_status","features","color","price_by_size",
        "additional_information","promo_message","price_promo","promo_description","online_exclusive",
        "extraction_date","image_url_large","image_url_small","base_unique_identifier","mpn","page_title",
        "pack_quantity","brand","shipping_text","reviews_count","average_rating","upc","technical_details",
        "meta_keywords","stock_availability","page_snapshot","item_condition","no_of_watchers","stock_status",
        "shipping_areas","shipping_weight","meta_description","canonical_url","meta_title","videos","add_on_item",
        "saleable_quantity","ship_surcharge","in_cart_price","add_to_cart_price","sku_variant","sku_id",
        "variation_url","availability","Frequency","UOM","region_site","region_input","base_id","currency_input",
        "scope","top_category","input_category","sub_category","images_chunk","Retailer","Project","Batch",
        "SubBatch","input_url","spid","normalizedpname","top_sku","brand_Input","brand_group","sku_type",
        "buffer_column_1","buffer_column_2","buffer_column_3","buffer_column_4","buffer_column_5"
    ],
    "COTY": [
        "top_category","category","sub_category","pid","key","RName","country","MPN","SKUVARIENT","SKUL","PName",
        "Division","Category","Department","Class","SubClass","Brand","RegularPrice","FinalPrice","PriceRange",
        "Availability","Rating","Review","Promotion","Position","Channel","currency","ProductImage","KeyWords",
        "day","month","year","Date","scope","Product_Description","base_id","region_input","currency_input",
        "stock_status","item_status","markdown_price","url","category_path","add_to_cart","multiple_url",
        "large_image","small_image","normalized_Brand","spid","video_from_carousel","video_from_body",
        "pdp+_content","features","specifications","seller_name","asin","upc"
    ],
    "Hermes": [
        "top_category","category","sub_category","pid","key","RName","country","MPN","SKUVARIENT","SKUL","PName",
        "Division","Category","Department","Class","SubClass","Brand","RegularPrice","FinalPrice","PriceRange",
        "Availability","Rating","Review","Promotion","Position","Channel","currency","ProductImage","KeyWords",
        "day","month","year","Date","scope","Product_Description","base_id","region_input","currency_input",
        "stock_status","item_status","markdown_price","url","category_path","add_to_cart","multiple_url",
        "large_image","small_image","normalized_Brand","spid","video_from_carousel","video_from_body",
        "pdp+_content","features","specifications","seller_name","asin","upc"
    ]
}

scope_values = set(df.get("scope", pd.Series([""])).astype(str).str.strip())
required_columns = None
scope_key_main = ""
for s in scope_values:
    scope_key = scope_key_from_value(s)
    if not scope_key_main:
        scope_key_main = scope_key
    for k, cols in REQUIRED_COLUMNS_BY_SCOPE.items():
        if k.lower() == scope_key:
            required_columns = cols
            break
    if required_columns:
        break

if required_columns:
    required_lower = [c.lower() for c in required_columns]
    current_cols = list(df.columns)
    current_lower = [c.lower() for c in current_cols]
    missing_required = [req for req, req_l in zip(required_columns, required_lower) if req_l not in current_lower]
    extra_current = [cur for cur, cur_l in zip(current_cols, current_lower) if cur_l not in set(required_lower)]
    # sequence check: compare the sequence of required columns as they appear in current columns
    current_required_sequence = [c for c in current_lower if c in set(required_lower)]
    sequence_ok = current_required_sequence == required_lower
    df["required_column_check"] = "PASS" if (len(missing_required) == 0 and sequence_ok) else "FAIL"
    df["missing_columns_check"] = "PASS" if len(missing_required) == 0 else "FAIL"
    df["extra_columns_check"] = "PASS" if len(extra_current) == 0 else "FAIL"
else:
    df["required_column_check"] = "PASS"
    df["missing_columns_check"] = "PASS"
    df["extra_columns_check"] = "PASS"

FAILURE_MESSAGE_MAP = {
    "rname_check": "rname should match as per master",
    "country_check": "country should match as per master",
    "row_count_check": "row count does not match crawl input for retailer",
    "date_check": "date should be current date",
    "price_rule_check": "price/status rule failed",
    "availability_check": "availability should match stock status (In Stock/Out of Stock)",
    "stock_na_check": "Not available found in columns while stock status is In Stock/Out of Stock",
    "rating_check": "rating format or value is invalid",
    "keywords_blank_check": "keywords column should be blank for this scope",
    "required_column_check": "all required columns not present",
    "missing_columns_check": "missing required columns present",
    "extra_columns_check": "extra columns present",
    "not_available_values_check": "Not available rule failed",
    "rating_normalization_check": "rating normalization failed (use X.0, 0 stays 0)",
    "rating_review_stock_check": "rating/review should not be Not available for In Stock/Out of Stock",
    "na_sku_input_check": "Not available SKU must match input PName/SKUVARIENT",
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
    if required_columns:
        current_cols = list(df.columns)
        req_df = pd.DataFrame([{
            "scope": next(iter(scope_values)) if scope_values else "",
            "required_columns": ", ".join(required_columns),
            "current_columns": ", ".join(current_cols),
            "overall_status": "FAIL" if df["required_column_check"].iloc[0] == "FAIL" else "PASS",
            "failure_message": "all required columns not present or order mismatch" if df["required_column_check"].iloc[0] == "FAIL" else ""
        }])
        req_df.to_excel(writer, sheet_name="Required_Columns_Check", index=False)
        detail_df = pd.DataFrame([{
            "scope": next(iter(scope_values)) if scope_values else "",
            "missing_columns": ", ".join(missing_required) if required_columns else "",
            "extra_columns": ", ".join(extra_current) if required_columns else "",
            "missing_columns_check": df["missing_columns_check"].iloc[0],
            "extra_columns_check": df["extra_columns_check"].iloc[0]
        }])
        detail_df.to_excel(writer, sheet_name="Missing_Extra_Columns", index=False)

print(f"✅ OUTPUT GENERATED: {OUTPUT_FILE}")
print("=== PDP CHECKLIST COMPLETED ===")
