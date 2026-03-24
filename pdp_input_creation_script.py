#!/usr/bin/env python3

import argparse
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parent
BUSINESS_DIR_NAME = "BUSSINESS TEAM PDP FILE"
CP_INPUT_DIR_NAME = "CP TOOL INPUT"
OUTPUT_DIR_NAME = "OUTPUT"

NA_MARKERS = {"", "n/a", "na", "null", "none", "nan"}

BIZ_ALIASES = {
    "base_id": ["base_id", "base id", "keyword_id", "keyword id", "ugam sku id"],
    "scope_name": ["scope", "scope_name", "scope name", "partner_id", "partner id", "bnc"],
    "retailer": ["retailer", "competitor", "rname", "partner_name", "domain_input", "site_name"],
    "retailer_url": [
        "retailer_url",
        "retailer url",
        "retailer url ",
        "retailer url/desktop url",
        "url",
        "product_url",
        "product page url",
        "product link",
    ],
    "brand": ["brand", "brand_name", "brand name"],
    "top_category": ["top_category", "top category"],
    "input_category": ["input_category", "input category", "category"],
    "sub_category": ["sub_category", "sub category", "subcategory"],
    "ean": [
        "ean",
        "upc",
        "mpn (upc)",
        "mpn_upc",
        "gtin",
        "barcode",
        "buffer_column_1",
        "ean/mpn (upc)",
        "ean_mpn_upc",
        "ean_mpn_upc_",
    ],
    "variation_1": ["variation_1", "variation 1", "variation1"],
    "variation_2": ["variation_2", "variation 2", "variation2", "variation_1_1"],
    "variation_3": ["variation_3", "variation 3", "variation3"],
    "mainurl": ["mainurl", "main_url", "main url", "retailer_url", "retailer url", "url"],
    "project": ["project", "project_name", "project name"],
    "normalizedpname": [
        "normalizedpname",
        "normalized_pname",
        "item",
        "item/product name",
        "product_title",
        "product_name",
        "product name",
        "sku_name_normalized_product_name_to_be_updated_by_ugam",
        "sku name normalized product name to be updated by ugam",
        "sku name",
    ],
    "top_sku": ["top_sku", "top sku", "topsku", "top sku flag(yes/ no)", "top sku flag"],
    "brand_input": ["brand_input", "brand input", "brand"],
    "brand_group": ["brand_group", "brand group"],
    "action": ["action", "instruction", "operation"],
}

CP_ALIASES = {
    "url": ["url", "product_url", "retailer_url", "retailer url", "retailer url "],
    "unique_identifier": ["uniqueidentifier", "unique_identifier", "unique id", "uid"],
    "base_id": ["base_id", "baseid", "base id", "keyword_id", "keyword id", "ugam sku id"],
    "scope_name": ["scope_name", "scope", "scope name", "partner_id", "partner id", "bnc"],
    "retailer": ["retailer", "competitor", "rname", "partner_name", "site_name", "domain_input"],
    "batch": ["batch", "batch_date", "batch date"],
    "domain_input": ["domain_input", "retailer", "competitor", "rname"],
    "partner_name": ["partner_name", "retailer", "competitor", "rname"],
    "site_name": ["site_name", "retailer", "competitor", "rname"],
    "partner_id": ["partner_id", "scope", "scope_name", "scope name", "bnc"],
    "keyword_id": ["keyword_id", "base_id", "base id", "ugam sku id"],
    "detailed_partner_id": ["detailed_partner_id", "retailer", "competitor", "rname"],
    "project": ["project"],
    "top_category": ["top_category", "top category"],
    "input_category": ["input_category", "input category", "category"],
    "sub_category": ["sub_category", "sub category"],
    "brand": ["brand"],
    "buffer_column_1": ["buffer_column_1", "ean", "upc", "mpn (upc)"],
    "variation_1": ["variation_1", "variation 1"],
    "variation_2": ["variation_2", "variation 2"],
    "variation_3": ["variation_3", "variation 3"],
}

RETAILER_PROFILE_FIELDS = [
    "domain",
    "webmethod",
    "type",
    "pagedepth",
    "gatewaytype",
    "validateparsedoutput",
    "fetchnextcrawlurl",
    "variation_flag",
    "region_input",
    "mainurl",
    "cookieparam",
    "cookie_input",
    "cookieparameter",
    "status",
    "frequency",
    "channel",
    "domain_input",
    "retailer",
    "project",
    "subbatch",
    "replication",
    "scrape_frequency",
    "partner_name",
    "site_name",
    "detailed_partner_id",
]


def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")


def normalize_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def is_na(value) -> bool:
    return normalize_text(value).lower() in NA_MARKERS


def as_na_or_text(value) -> str:
    text = normalize_text(value)
    return "n/a" if text == "" else text


def key_norm(value) -> str:
    return normalize_text(value).lower()


def strip_generated_id(url: str) -> str:
    return re.sub(r"#id=\d+$", "", normalize_text(url))


class ColumnResolver:
    def __init__(self, columns):
        self.columns = list(columns)
        self.norm_to_actual = {}
        for col in self.columns:
            norm = normalize_name(col)
            if norm not in self.norm_to_actual:
                self.norm_to_actual[norm] = col

    def find(self, aliases):
        for alias in aliases:
            col = self.norm_to_actual.get(normalize_name(alias))
            if col is not None:
                return col
        return None

    def require(self, aliases, label):
        col = self.find(aliases)
        if col is None:
            raise ValueError(f"Missing required column for '{label}'. Tried aliases: {aliases}")
        return col


def parse_args():
    parser = argparse.ArgumentParser(
        description="Create PDP input rows from business team file and CP tool input file."
    )
    parser.add_argument("--root", default=str(ROOT_DIR), help="Root folder containing default subfolders.")
    parser.add_argument("--business-file", default="", help="Optional explicit business file path.")
    parser.add_argument("--cp-file", default="", help="Optional explicit CP tool input file path.")
    parser.add_argument(
        "--go-live-date",
        default="",
        help="Go-live date for Batch field in YYYY-MM-DD. Defaults to today.",
    )
    parser.add_argument(
        "--mapping-file",
        default="",
        help="Optional mapping database file path for URL and variation updates.",
    )
    return parser.parse_args()


def ensure_default_dirs(root: Path):
    business_dir = root / BUSINESS_DIR_NAME
    cp_dir = root / CP_INPUT_DIR_NAME
    output_dir = root / OUTPUT_DIR_NAME
    business_dir.mkdir(parents=True, exist_ok=True)
    cp_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return business_dir, cp_dir, output_dir


def pick_latest_file(folder: Path, suffixes):
    files = [
        p
        for p in folder.iterdir()
        if p.is_file() and p.suffix.lower() in suffixes and not p.name.startswith("~$")
    ]
    if not files:
        raise FileNotFoundError(f"No matching files found in: {folder}")
    return max(files, key=lambda p: p.stat().st_mtime)


def read_business_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".tsv", ".txt"}:
        df = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
    elif suffix == ".csv":
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    else:
        df = pd.read_excel(path, dtype=str, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    return df.fillna("")


def read_cp_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".tsv", ".txt"}:
        df = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
    elif suffix == ".csv":
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    else:
        df = pd.read_excel(path, dtype=str, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    return df.fillna("")


def resolve_business_columns(df: pd.DataFrame):
    resolver = ColumnResolver(df.columns)
    cols = {}
    cols["base_id"] = resolver.require(BIZ_ALIASES["base_id"], "business base_id")
    cols["retailer"] = resolver.require(BIZ_ALIASES["retailer"], "business retailer")
    cols["retailer_url"] = resolver.require(BIZ_ALIASES["retailer_url"], "business retailer url")
    cols["scope_name"] = resolver.find(BIZ_ALIASES["scope_name"])

    optional_fields = [
        "brand",
        "top_category",
        "input_category",
        "sub_category",
        "ean",
        "variation_1",
        "variation_2",
        "variation_3",
        "mainurl",
        "project",
        "normalizedpname",
        "top_sku",
        "brand_input",
        "brand_group",
        "action",
    ]
    for field in optional_fields:
        cols[field] = resolver.find(BIZ_ALIASES[field])
    return cols


def resolve_cp_columns(df: pd.DataFrame):
    resolver = ColumnResolver(df.columns)
    cols = {}
    for key, aliases in CP_ALIASES.items():
        if key in {"url", "unique_identifier", "base_id", "scope_name", "retailer", "batch"}:
            cols[key] = resolver.require(aliases, f"cp {key}")
        else:
            cols[key] = resolver.find(aliases)
    return resolver, cols


def parse_date_or_today(date_text: str) -> datetime:
    if not date_text:
        return datetime.today()
    return datetime.strptime(date_text, "%Y-%m-%d")


def build_output_name(cp_input_name: str, go_live_dt: datetime) -> str:
    date_token = go_live_dt.strftime("%Y%m%d")
    m = re.search(r"(.*?)(\d{8})(\.[^.]+)$", cp_input_name)
    if m:
        return f"{m.group(1)}{date_token}{m.group(3)}"

    stem = Path(cp_input_name).stem
    suffix = Path(cp_input_name).suffix or ".tsv"
    return f"{stem}_{date_token}{suffix}"


def set_if_exists(row: dict, col: str, value):
    if col is not None:
        row[col] = value


def fetch_business_value(biz_row, col_name):
    if col_name is None:
        return ""
    return normalize_text(biz_row.get(col_name, ""))


def read_mapping_db(path: Path) -> pd.DataFrame:
    """Read DB sheet from mapping database file."""
    try:
        df = pd.read_excel(path, sheet_name="DB", dtype=str, engine="openpyxl")
        df.columns = [str(c).strip() for c in df.columns]
        return df.fillna("")
    except Exception as e:
        print(f"Warning: Could not read mapping database: {e}")
        return None


def build_mapping_index(mapping_df, resolver) -> dict:
    """Build lookup index from (base_id, scope, retailer) to row."""
    if mapping_df is None or mapping_df.empty:
        return {}
    
    mapping_cols = {}
    mapping_cols["base_id"] = resolver.find(["base_id"])
    mapping_cols["scope"] = resolver.find(["scope"])
    mapping_cols["retailer"] = resolver.find(["retailer"])
    
    index = {}
    for _, row in mapping_df.iterrows():
        base_id = key_norm(row.get(mapping_cols["base_id"], "")) if mapping_cols["base_id"] else ""
        scope = key_norm(row.get(mapping_cols["scope"], "")) if mapping_cols["scope"] else ""
        retailer = key_norm(row.get(mapping_cols["retailer"], "")) if mapping_cols["retailer"] else ""
        
        if base_id and scope and retailer:
            index[(base_id, scope, retailer)] = row
    
    return index, mapping_cols


def get_mapping_updates(mapping_row, mapping_cols, cp_cols, cp_opt):
    """Extract URL and variation updates from mapping row."""
    updates = {}
    
    # Check status column
    status_col = None
    for col in mapping_row.index:
        if normalize_name(col) == normalize_name("status"):
            status_col = col
            break
    
    if status_col is None:
        return updates
    
    status = normalize_text(mapping_row.get(status_col, "")).lower()
    
    # Only process if status indicates update
    if "update" not in status:
        return updates
    
    # Determine what to update
    update_url = "url" in status and "variation" not in status or ("url" in status and "variation" in status)
    update_variation = "variation" in status
    
    # Get updated values
    if update_url:
        for col in mapping_row.index:
            if normalize_name(col).startswith("updated_url") or col == "Updated URL":
                updated_url = normalize_text(mapping_row.get(col, ""))
                if updated_url and not is_na(updated_url):
                    if cp_cols.get("url"):
                        updates["url"] = updated_url
                    break
    
    if update_variation:
        for var_num in [1, 2, 3]:
            for col in mapping_row.index:
                col_norm = normalize_name(col)
                if col_norm == f"updated_variation_{var_num}" or col == f"Updated Variation {var_num}":
                    updated_var = normalize_text(mapping_row.get(col, ""))
                    if updated_var and not is_na(updated_var):
                        cp_col_key = f"variation_{var_num}"
                        if cp_opt.get(cp_col_key):
                            updates[cp_col_key] = updated_var
                    break
    
    return updates


def main():
    args = parse_args()
    root = Path(args.root).resolve()
    business_dir, cp_dir, output_dir = ensure_default_dirs(root)

    business_file = Path(args.business_file).resolve() if args.business_file else pick_latest_file(
        business_dir, {".xlsx", ".xls", ".csv", ".tsv", ".txt"}
    )
    cp_file = Path(args.cp_file).resolve() if args.cp_file else pick_latest_file(
        cp_dir, {".tsv", ".txt", ".csv", ".xlsx", ".xls"}
    )

    go_live_dt = parse_date_or_today(args.go_live_date)
    batch_value = go_live_dt.strftime("%d-%b-%y")

    print(f"Business file: {business_file}")
    print(f"CP input file: {cp_file}")
    print(f"Batch date   : {batch_value}")

    biz_df = read_business_file(business_file)
    cp_df = read_cp_file(cp_file)

    biz_cols = resolve_business_columns(biz_df)
    cp_resolver, cp_cols = resolve_cp_columns(cp_df)

    # Load mapping database if provided
    mapping_df = None
    mapping_index = {}
    mapping_cols = {}
    if args.mapping_file:
        mapping_file = Path(args.mapping_file).resolve()
        if mapping_file.exists():
            mapping_df = read_mapping_db(mapping_file)
            if mapping_df is not None and not mapping_df.empty:
                mapping_index, mapping_cols = build_mapping_index(mapping_df, cp_resolver)
                print(f"Mapping file: {mapping_file} ({len(mapping_index)} mappings loaded)")
        else:
            print(f"Warning: Mapping file not found: {mapping_file}")

    cp_records = cp_df.to_dict("records")

    # Scope fallback for business files without explicit scope column.
    scope_fallback = ""
    if biz_cols["scope_name"] is None:
        unique_scopes = sorted(
            {
                normalize_text(r.get(cp_cols["scope_name"], ""))
                for r in cp_records
                if normalize_text(r.get(cp_cols["scope_name"], "")) != ""
            }
        )
        if len(unique_scopes) == 1:
            scope_fallback = unique_scopes[0]
            print(f"Business scope column missing; using CP scope fallback: {scope_fallback}")
        else:
            raise ValueError(
                "Business file has no scope column and CP input has multiple scopes. "
                "Please provide scope in the business file."
            )

    # Build indices.
    base_templates_by_scope = {}
    base_templates_fallback = {}
    retailer_profiles = {}

    retailer_key_col = cp_cols.get("partner_name") or cp_cols["retailer"] or cp_cols.get("domain_input")

    for rec in cp_records:
        scope = normalize_text(rec.get(cp_cols["scope_name"], ""))
        base_id = normalize_text(rec.get(cp_cols["base_id"], ""))
        retailer_key = normalize_text(rec.get(retailer_key_col, ""))

        if scope and base_id and (scope, base_id) not in base_templates_by_scope:
            base_templates_by_scope[(scope, base_id)] = rec
        if base_id and base_id not in base_templates_fallback:
            base_templates_fallback[base_id] = rec
        if retailer_key and key_norm(retailer_key) not in retailer_profiles:
            retailer_profiles[key_norm(retailer_key)] = rec

    # Existing uniqueness key.
    existing_keys = set()
    for rec in cp_records:
        scope = key_norm(rec.get(cp_cols["scope_name"], ""))
        base_id = key_norm(rec.get(cp_cols["base_id"], ""))
        retailer = key_norm(rec.get(cp_cols["retailer"], ""))
        url = strip_generated_id(rec.get(cp_cols["url"], ""))
        existing_keys.add((scope, base_id, retailer, url))

    # Unique ID seed.
    max_uid = 0
    for rec in cp_records:
        raw_uid = normalize_text(rec.get(cp_cols["unique_identifier"], ""))
        if raw_uid.isdigit():
            max_uid = max(max_uid, int(raw_uid))

    # Pre-resolve known optional CP columns.
    cp_opt = {}
    opt_names = [
        "mainurl",
        "cookieparam",
        "cookie_input",
        "cookieparameter",
        "color",
        "variation_1",
        "variation_2",
        "variation_3",
        "status",
        "frequency",
        "channel",
        "top_category",
        "input_category",
        "sub_category",
        "brand",
        "normalizedpname",
        "top_sku",
        "brand_input",
        "brand_group",
        "buffer_column_1",
        "buffer_column_2",
        "buffer_column_3",
        "buffer_column_4",
        "buffer_column_5",
        "sku_type",
        "project",
        "subbatch",
        "replication",
        "scrape_frequency",
        "partner_name",
        "site_name",
        "partner_id",
        "keyword_id",
        "detailed_partner_id",
        "domain_input",
        "universe",
        "category",
        "product_name",
        "product_model",
        "cadence",
    ]
    for opt in opt_names:
        cp_opt[opt] = cp_resolver.find([opt])

    def get_dedupe_key(row):
        scope = key_norm(row.get(cp_cols["scope_name"], ""))
        base_id = key_norm(row.get(cp_cols["base_id"], ""))
        retailer = key_norm(row.get(cp_cols["retailer"], ""))
        url = strip_generated_id(row.get(cp_cols["url"], ""))
        return (scope, base_id, retailer, url)

    new_rows = []
    skipped_existing = 0
    skipped_invalid = 0
    missing_base_template = 0
    missing_retailer_profile = 0
    remove_keys = set()

    for biz_row in biz_df.to_dict("records"):
        base_id = fetch_business_value(biz_row, biz_cols["base_id"])
        retailer = fetch_business_value(biz_row, biz_cols["retailer"])
        retailer_url = fetch_business_value(biz_row, biz_cols["retailer_url"])

        if biz_cols["scope_name"] is not None:
            scope = fetch_business_value(biz_row, biz_cols["scope_name"])
        else:
            scope = scope_fallback

        action = fetch_business_value(biz_row, biz_cols.get("action")).lower()
        if action in ["remove", "removal", "delete"]:
            dedupe_key = (key_norm(scope), key_norm(base_id), key_norm(retailer), strip_generated_id(retailer_url))
            remove_keys.add(dedupe_key)
            continue

        if not base_id or not retailer or not retailer_url or not scope:
            skipped_invalid += 1
            continue

        dedupe_key = (key_norm(scope), key_norm(base_id), key_norm(retailer), strip_generated_id(retailer_url))
        if dedupe_key in existing_keys:
            skipped_existing += 1
            continue

        base_template = base_templates_by_scope.get((scope, base_id)) or base_templates_fallback.get(base_id)
        if base_template is None:
            missing_base_template += 1
            base_template = {}

        retailer_profile = retailer_profiles.get(key_norm(retailer))
        if retailer_profile is None:
            missing_retailer_profile += 1
            retailer_profile = {}

        # Start from base template to keep SKU-level values stable.
        new_row = {
            col: as_na_or_text(base_template.get(col, ""))
            for col in cp_df.columns
        }

        # Apply retailer-profile values for retailer/script specific fields.
        for alias in RETAILER_PROFILE_FIELDS:
            col = cp_resolver.find([alias])
            if col is not None:
                prof_val = retailer_profile.get(col, "")
                if normalize_text(prof_val) != "":
                    new_row[col] = as_na_or_text(prof_val)

        max_uid += 1
        unique_id = str(max_uid)
        url_with_id = f"{strip_generated_id(retailer_url)}#id={unique_id}"

        # Required mappings.
        new_row[cp_cols["unique_identifier"]] = unique_id
        new_row[cp_cols["url"]] = url_with_id
        new_row[cp_cols["base_id"]] = base_id
        new_row[cp_cols["scope_name"]] = scope
        new_row[cp_cols["retailer"]] = retailer
        new_row[cp_cols["batch"]] = batch_value

        # Business overrides when available.
        for biz_key, cp_key in [
            ("brand", "brand"),
            ("top_category", "top_category"),
            ("input_category", "input_category"),
            ("sub_category", "sub_category"),
            ("normalizedpname", "normalizedpname"),
            ("top_sku", "top_sku"),
            ("brand_group", "brand_group"),
            ("ean", "buffer_column_1"),
        ]:
            b_val = fetch_business_value(biz_row, biz_cols.get(biz_key))
            cp_col = cp_opt.get(cp_key)
            if cp_col is not None and not is_na(b_val):
                new_row[cp_col] = b_val

        brand_input_val = fetch_business_value(biz_row, biz_cols.get("brand_input"))
        if is_na(brand_input_val):
            brand_input_val = fetch_business_value(biz_row, biz_cols.get("brand"))
        if cp_opt.get("brand_input") is not None and not is_na(brand_input_val):
            new_row[cp_opt["brand_input"]] = brand_input_val

        var1 = fetch_business_value(biz_row, biz_cols.get("variation_1"))
        var2 = fetch_business_value(biz_row, biz_cols.get("variation_2"))
        var3 = fetch_business_value(biz_row, biz_cols.get("variation_3"))
        set_if_exists(new_row, cp_opt["variation_1"], as_na_or_text(var1))
        set_if_exists(new_row, cp_opt["variation_2"], as_na_or_text(var2))
        set_if_exists(new_row, cp_opt["variation_3"], as_na_or_text(var3))

        # Check mapping database for URL and variation updates
        if mapping_index:
            mapping_key = (key_norm(scope), key_norm(base_id), key_norm(retailer))
            if mapping_key in mapping_index:
                mapping_row = mapping_index[mapping_key]
                mapping_updates = get_mapping_updates(mapping_row, mapping_cols, cp_cols, cp_opt)
                
                # Apply mapping updates
                if "url" in mapping_updates and cp_cols.get("url"):
                    new_row[cp_cols["url"]] = f"{mapping_updates['url']}#id={unique_id}"
                for var_num in [1, 2, 3]:
                    var_key = f"variation_{var_num}"
                    if var_key in mapping_updates and cp_opt.get(var_key):
                        new_row[cp_opt[var_key]] = mapping_updates[var_key]

        # Fixed/default fields from the process.
        set_if_exists(new_row, cp_opt["cookieparam"], "n/a")
        set_if_exists(new_row, cp_opt["cookie_input"], "n/a")
        set_if_exists(new_row, cp_opt["color"], "n/a")
        set_if_exists(new_row, cp_opt["status"], "n/a")
        set_if_exists(new_row, cp_opt["sku_type"], "n/a")
        set_if_exists(new_row, cp_opt["buffer_column_2"], "n/a")
        set_if_exists(new_row, cp_opt["buffer_column_3"], "n/a")
        set_if_exists(new_row, cp_opt["buffer_column_4"], "n/a")
        set_if_exists(new_row, cp_opt["buffer_column_5"], "n/a")
        set_if_exists(new_row, cp_opt["subbatch"], "Final")
        set_if_exists(new_row, cp_opt["replication"], "Yes")
        set_if_exists(new_row, cp_opt["scrape_frequency"], "Daily")
        set_if_exists(new_row, cp_opt["partner_id"], scope)
        set_if_exists(new_row, cp_opt["keyword_id"], base_id)
        set_if_exists(new_row, cp_opt["detailed_partner_id"], retailer)
        set_if_exists(new_row, cp_opt["partner_name"], retailer)
        set_if_exists(new_row, cp_opt["site_name"], retailer)
        set_if_exists(new_row, cp_opt["domain_input"], retailer)
        set_if_exists(new_row, cp_opt["universe"], "n/a")
        set_if_exists(new_row, cp_opt["category"], "n/a")
        set_if_exists(new_row, cp_opt["product_name"], "n/a")
        set_if_exists(new_row, cp_opt["product_model"], "n/a")
        set_if_exists(new_row, cp_opt["cadence"], "n/a")

        # Optional mainurl/project from business if explicitly provided.
        biz_mainurl = fetch_business_value(biz_row, biz_cols.get("mainurl"))
        if cp_opt["mainurl"] is not None and not is_na(biz_mainurl):
            new_row[cp_opt["mainurl"]] = biz_mainurl

        biz_project = fetch_business_value(biz_row, biz_cols.get("project"))
        if cp_opt["project"] is not None and not is_na(biz_project):
            new_row[cp_opt["project"]] = biz_project

        # Final cleanup for blanks.
        for col in cp_df.columns:
            if normalize_text(new_row.get(col, "")) == "":
                new_row[col] = "n/a"

        new_rows.append(new_row)
        existing_keys.add(dedupe_key)

    cp_df_filtered = cp_df[~cp_df.apply(lambda row: get_dedupe_key(row) in remove_keys, axis=1)]

    output_name = build_output_name(cp_file.name, go_live_dt)
    output_path = output_dir / output_name

    output_df = pd.concat([cp_df_filtered, pd.DataFrame(new_rows)], ignore_index=True)
    output_df.to_csv(output_path, sep="\t", index=False, encoding="utf-8")

    print("\n=== PDP INPUT CREATION SUMMARY ===")
    print(f"CP rows (original): {len(cp_df)}")
    print(f"Rows added       : {len(new_rows)}")
    print(f"Rows removed     : {len(remove_keys)}")
    print(f"Rows skipped (existing): {skipped_existing}")
    print(f"Rows skipped (invalid) : {skipped_invalid}")
    print(f"Missing base template rows: {missing_base_template}")
    print(f"Missing retailer profiles: {missing_retailer_profile}")
    print(f"Output rows      : {len(output_df)}")
    print(f"Output file      : {output_path}")


if __name__ == "__main__":
    main()
