#!/usr/bin/env python3

import argparse
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parent
CP_INPUT_DIR_NAME = "CP TOOL INPUT"
MAPPING_DB_FILE_NAME = "Internal_COTY_Benelux_Consumer Beauty_Database_DAMA.xlsx"
OUTPUT_DIR_NAME = "OUTPUT"


def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")


def normalize_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def is_na(value) -> bool:
    na_markers = {"", "n/a", "na", "null", "none", "nan"}
    return normalize_text(value).lower() in na_markers


def key_norm(value) -> str:
    return normalize_text(value).lower()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Update PDP input rows with URL and variation from mapping database."
    )
    parser.add_argument("--root", default=str(ROOT_DIR), help="Root folder containing default subfolders.")
    parser.add_argument("--cp-file", default="", help="Optional explicit CP tool input file path.")
    parser.add_argument("--mapping-file", default="", help="Optional explicit mapping database file path.")
    parser.add_argument(
        "--go-live-date",
        default="",
        help="Go-live date for Batch field in YYYY-MM-DD. Defaults to today.",
    )
    return parser.parse_args()


def ensure_default_dirs(root: Path):
    cp_dir = root / CP_INPUT_DIR_NAME
    output_dir = root / OUTPUT_DIR_NAME
    cp_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return cp_dir, output_dir


def pick_latest_file(folder: Path, suffixes):
    files = [
        p
        for p in folder.iterdir()
        if p.is_file() and p.suffix.lower() in suffixes and not p.name.startswith("~$")
    ]
    if not files:
        raise FileNotFoundError(f"No matching files found in: {folder}")
    return max(files, key=lambda p: p.stat().st_mtime)


def find_mapping_db(root: Path):
    """Find mapping database file in root or subdirectories."""
    if (root / MAPPING_DB_FILE_NAME).exists():
        return root / MAPPING_DB_FILE_NAME
    
    # Search in subdirectories
    for filepath in root.glob(f"**/{MAPPING_DB_FILE_NAME}"):
        if filepath.is_file():
            return filepath
    
    raise FileNotFoundError(f"Mapping database file '{MAPPING_DB_FILE_NAME}' not found in {root}")


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


def read_mapping_db(path: Path) -> pd.DataFrame:
    """Read DB sheet from mapping database file."""
    df = pd.read_excel(path, sheet_name="DB", dtype=str, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    return df.fillna("")


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


def resolve_cp_columns(df: pd.DataFrame):
    """Resolve required CP columns."""
    resolver = ColumnResolver(df.columns)
    cols = {}
    
    # Required columns
    cols["base_id"] = resolver.require(
        ["base_id", "baseid", "base id", "keyword_id", "keyword id", "ugam sku id"],
        "base_id"
    )
    cols["scope_name"] = resolver.require(
        ["scope_name", "scope", "scope name", "partner_id", "partner id", "bnc"],
        "scope_name"
    )
    cols["retailer"] = resolver.require(
        ["retailer", "competitor", "rname", "partner_name", "site_name", "domain_input"],
        "retailer"
    )
    cols["url"] = resolver.require(
        ["url", "product_url", "retailer_url", "retailer url", "retailer url "],
        "url"
    )
    
    # Optional columns
    cols["variation_1"] = resolver.find(["variation_1", "variation 1"])
    cols["variation_2"] = resolver.find(["variation_2", "variation 2"])
    cols["variation_3"] = resolver.find(["variation_3", "variation 3"])
    
    return resolver, cols


def resolve_mapping_columns(df: pd.DataFrame):
    """Resolve mapping database columns."""
    resolver = ColumnResolver(df.columns)
    cols = {}
    
    cols["base_id"] = resolver.require(["base_id"], "mapping base_id")
    cols["scope"] = resolver.require(["scope"], "mapping scope")
    cols["retailer"] = resolver.require(["retailer"], "mapping retailer")
    cols["status"] = resolver.require(["status"], "mapping status")
    cols["updated_url"] = resolver.find(["updated url", "updated_url"])
    cols["updated_variation_1"] = resolver.find(["updated variation 1", "updated_variation_1"])
    cols["updated_variation_2"] = resolver.find(["updated variation 2", "updated_variation_2"])
    cols["updated_variation_3"] = resolver.find(["updated variation 3", "updated_variation_3"])
    
    return cols


def build_output_name(cp_input_name: str, go_live_dt: datetime, suffix: str = "_URLVariationUpdate") -> str:
    """Build output filename with date and update marker."""
    date_token = go_live_dt.strftime("%Y%m%d")
    m = re.search(r"(.*?)(\d{8})(\.[^.]+)$", cp_input_name)
    if m:
        base_name = m.group(1).rstrip("_")
        return f"{base_name}{suffix}_{date_token}{m.group(3)}"
    
    stem = Path(cp_input_name).stem
    file_suffix = Path(cp_input_name).suffix or ".tsv"
    return f"{stem}{suffix}_{date_token}{file_suffix}"


def parse_update_type(status_str: str) -> tuple:
    """
    Parse status column to determine what needs updating.
    Returns: (update_url, update_variation)
    """
    status_lower = normalize_text(status_str).lower()
    
    update_url = False
    update_variation = False
    
    if "url" in status_lower and "variation" in status_lower:
        # "URL, Variation Updated" or similar
        update_url = True
        update_variation = True
    elif "url" in status_lower:
        # "URL Updated"
        update_url = True
    elif "variation" in status_lower:
        # "Variation Updated"
        update_variation = True
    
    return update_url, update_variation


def main():
    args = parse_args()
    root = Path(args.root).resolve()
    cp_dir, output_dir = ensure_default_dirs(root)

    cp_file = Path(args.cp_file).resolve() if args.cp_file else pick_latest_file(
        cp_dir, {".tsv", ".txt", ".csv", ".xlsx", ".xls"}
    )
    
    mapping_file = Path(args.mapping_file).resolve() if args.mapping_file else find_mapping_db(root)

    go_live_dt = datetime.strptime(args.go_live_date, "%Y-%m-%d") if args.go_live_date else datetime.today()

    print(f"CP input file    : {cp_file}")
    print(f"Mapping DB file  : {mapping_file}")
    print(f"Go-live date     : {go_live_dt.strftime('%Y-%m-%d')}")

    # Read files
    cp_df = read_cp_file(cp_file)
    mapping_df = read_mapping_db(mapping_file)

    # Resolve columns
    cp_resolver, cp_cols = resolve_cp_columns(cp_df)
    mapping_cols = resolve_mapping_columns(mapping_df)

    print(f"\nCP rows (input)    : {len(cp_df)}")
    print(f"Mapping DB rows    : {len(mapping_df)}")

    # Build mapping index: (Base_ID, Scope, Retailer) -> row
    mapping_index = {}
    for idx, row in mapping_df.iterrows():
        base_id = key_norm(row.get(mapping_cols["base_id"], ""))
        scope = key_norm(row.get(mapping_cols["scope"], ""))
        retailer = key_norm(row.get(mapping_cols["retailer"], ""))
        
        if base_id and scope and retailer:
            key = (base_id, scope, retailer)
            mapping_index[key] = row

    # Process CP rows
    updated_rows = 0
    url_updates = 0
    variation_updates = 0
    skipped_no_match = 0
    skipped_no_update = 0

    output_rows = []

    for idx, cp_row in cp_df.iterrows():
        # Create a mutable copy
        output_row = cp_row.to_dict()
        
        # Extract identifiers
        base_id = key_norm(cp_row.get(cp_cols["base_id"], ""))
        scope = key_norm(cp_row.get(cp_cols["scope_name"], ""))
        retailer = key_norm(cp_row.get(cp_cols["retailer"], ""))

        mapping_key = (base_id, scope, retailer)

        if mapping_key not in mapping_index:
            skipped_no_match += 1
            output_rows.append(output_row)
            continue

        mapping_row = mapping_index[mapping_key]
        status = normalize_text(mapping_row.get(mapping_cols["status"], ""))

        # Check if this row needs updating
        status_lower = status.lower()
        if "update" not in status_lower:
            skipped_no_update += 1
            output_rows.append(output_row)
            continue

        # Parse what needs updating
        update_url, update_variation = parse_update_type(status)

        # Update URL if needed
        if update_url and mapping_cols["updated_url"] is not None:
            updated_url = normalize_text(mapping_row.get(mapping_cols["updated_url"], ""))
            if updated_url and not is_na(updated_url):
                output_row[cp_cols["url"]] = updated_url
                url_updates += 1

        # Update variations if needed
        if update_variation:
            for var_num in [1, 2, 3]:
                map_col_key = f"updated_variation_{var_num}"
                cp_col_key = f"variation_{var_num}"
                
                if mapping_cols.get(map_col_key) is not None and cp_cols.get(cp_col_key) is not None:
                    updated_var = normalize_text(mapping_row.get(mapping_cols[map_col_key], ""))
                    if updated_var and not is_na(updated_var):
                        output_row[cp_cols[cp_col_key]] = updated_var
                        variation_updates += 1

        updated_rows += 1
        output_rows.append(output_row)

    # Build output dataframe
    output_df = pd.DataFrame(output_rows)

    # Save output
    output_name = build_output_name(cp_file.name, go_live_dt)
    output_path = output_dir / output_name

    output_df.to_csv(output_path, sep="\t", index=False, encoding="utf-8")

    print("\n=== PDP URL & VARIATION UPDATE SUMMARY ===")
    print(f"CP rows (input)         : {len(cp_df)}")
    print(f"Rows matched in mapping : {updated_rows}")
    print(f"Rows skipped (no match) : {skipped_no_match}")
    print(f"Rows skipped (no update): {skipped_no_update}")
    print(f"URL updates applied     : {url_updates}")
    print(f"Variation updates applied: {variation_updates}")
    print(f"Output file             : {output_path}")
    print("=== UPDATE COMPLETE ===")


if __name__ == "__main__":
    main()
