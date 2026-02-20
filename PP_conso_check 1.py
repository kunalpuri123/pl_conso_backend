import argparse
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime

import pandas as pd
import win32com.client as win32
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

REFERENCE_EXCEL = r"file_name_format.xlsx"
AE_CHECK_FOLDER = r"AE_Checks"


def load_tsv(path):
    return pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False, encoding="utf-8")


def save_tsv(df, path):
    df.to_csv(path, sep="\t", index=False, encoding="utf-8")


def clean_text(col):
    return (
        col.str.replace('"', "", regex=False)
        .str.replace(",", " ", regex=False)
        .str.replace("  ", " ", regex=False)
    )


def normalize_name(name):
    if not name:
        return ""
    name = name.strip()
    if name.lower().endswith(".tsv"):
        name = name[:-4]
    name = re.sub(r"\d{4}-\d{2}-\d{2}", "", name)
    name = re.sub(r"\s+", " ", name)
    return name.lower().strip()


def today_compact():
    return datetime.today().strftime("%Y%m%d")


def resolve_ae_naming(conso_filename, reference_excel):
    ref_df = pd.read_excel(reference_excel, dtype=str).fillna("")
    normalized_current = normalize_name(conso_filename)

    matched_idx = None
    for idx, row in ref_df.iterrows():
        if normalize_name(row["conso_file"]) == normalized_current:
            matched_idx = idx
            break

    if matched_idx is None:
        raise ValueError(
            f"Conso filename '{conso_filename}' not found in '{reference_excel}'. "
            "Please add conso_file, final_name_template, and AE_check_file_name mapping first."
        )

    row = ref_df.loc[matched_idx]
    final_name_template = str(row["final_name_template"]).strip()
    ae_file_name = str(row["AE_check_file_name"]).strip()
    return final_name_template, ae_file_name


def run_conso_check_once(file_path):
    df = load_tsv(file_path)
    checklist = []

    if "isRemoved" in df.columns:
        removed = df[df["isRemoved"] == "Yes"]
        removed_banners = sorted(removed["banner_name"].dropna().unique().tolist()) if "banner_name" in removed.columns else []
        checklist.append(
            {
                "check_name": "isRemoved_rows_removed",
                "status": "PASS",
                "failed_count": 0,
                "details": f"Removed rows dropped: {len(removed)}; banners: {', '.join(removed_banners)}",
            }
        )
        df = df[df["isRemoved"] != "Yes"]

    if "isSaved" in df.columns:
        invalid = df[df["isSaved"] != "Yes"]
        failed_banners = sorted(invalid["banner_name"].dropna().unique().tolist()) if "banner_name" in invalid.columns else []
        checklist.append(
            {
                "check_name": "isSaved_must_be_yes",
                "status": "FAIL" if not invalid.empty else "PASS",
                "failed_count": int(len(invalid)),
                "details": ", ".join(failed_banners),
            }
        )

    mandatory_cols = ["date", "country", "channel", "brand_name", "retailer_name"]
    mandatory_fail_msgs = []
    mandatory_fail_count = 0
    for col in mandatory_cols:
        if col in df.columns:
            bad = df[df[col].astype(str).str.upper() == "NA"]
            if not bad.empty:
                mandatory_fail_count += int(len(bad))
                banners = sorted(bad["banner_name"].dropna().unique().tolist()) if "banner_name" in bad.columns else []
                mandatory_fail_msgs.append(f"{col}: {', '.join(banners)}")

    checklist.append(
        {
            "check_name": "mandatory_columns_no_na",
            "status": "FAIL" if mandatory_fail_count > 0 else "PASS",
            "failed_count": mandatory_fail_count,
            "details": " | ".join(mandatory_fail_msgs),
        }
    )

    if df.shape[1] > 7:
        df = df.iloc[:, 7:]

    promo_cols = {"is_it_promotional", "promo_message"}
    if promo_cols.issubset(df.columns):
        invalid = df[
            ((df["is_it_promotional"] == "1") & (df["promo_message"] == "NA"))
            | ((df["is_it_promotional"] == "0") & (df["promo_message"] != "NA"))
        ]
        failed_banners = sorted(invalid["banner_name"].dropna().unique().tolist()) if "banner_name" in invalid.columns else []
        checklist.append(
            {
                "check_name": "promo_message_consistency",
                "status": "FAIL" if not invalid.empty else "PASS",
                "failed_count": int(len(invalid)),
                "details": ", ".join(failed_banners),
            }
        )

    for col in ["banner_alt_text", "promo_message", "exposed_sku"]:
        if col in df.columns:
            df[col] = clean_text(df[col].astype(str))

    if {"brand_name", "exposed_sku"}.issubset(df.columns):
        df.loc[df["brand_name"] == "No Brand", "exposed_sku"] = "NA"

    has_fail = any(row["status"] == "FAIL" for row in checklist)
    return df, checklist, has_fail


def create_values_file(ae_local_path):
    excel = win32.DispatchEx("Excel.Application")
    excel.Visible = False
    excel.DisplayAlerts = False

    wb = excel.Workbooks.Open(ae_local_path, ReadOnly=False)
    excel.CalculateFull()

    values_path = ae_local_path.replace(".xlsx", "_VALUES.xlsx")
    wb.SaveAs(values_path, FileFormat=51)

    wb.Close(False)
    excel.Quit()
    return values_path


def clear_brand_division_zeros(ae_path, values_path):
    for _ in range(5):
        try:
            wb_ae = load_workbook(ae_path)
            break
        except PermissionError:
            time.sleep(0.5)
    else:
        raise PermissionError(f"File still locked: {ae_path}")

    wb_val = load_workbook(values_path, data_only=True)

    final_sheet = next(s for s in wb_ae.sheetnames if s.lower() == "final")
    ws_final = wb_ae[final_sheet]
    ws_val = wb_val[final_sheet]

    headers = [ws_final.cell(1, c).value for c in range(1, ws_final.max_column + 1)]
    if "brand_division" not in headers:
        wb_ae.close()
        wb_val.close()
        return

    col = headers.index("brand_division") + 1
    for r in range(2, ws_final.max_row + 1):
        val = ws_val.cell(r, col).value
        if str(val).strip().lower() in {"0", "0.0"}:
            ws_final.cell(r, col, "")

    wb_ae.save(ae_path)
    wb_ae.close()
    wb_val.close()


def validate_checks_using_values(ae_path, values_path):
    for _ in range(5):
        try:
            wb_ae = load_workbook(ae_path)
            break
        except PermissionError:
            time.sleep(0.5)
    else:
        raise PermissionError(f"File still locked: {ae_path}")

    wb_val = load_workbook(values_path, data_only=True)

    checks = next(s for s in wb_ae.sheetnames if s.lower() == "checks")
    final = next(s for s in wb_ae.sheetnames if s.lower() == "final")

    ws_checks = wb_ae[checks]
    ws_values = wb_val[checks]
    ws_final = wb_ae[final]

    highlight = PatternFill("solid", fgColor="FFFF00")
    clear_fill = PatternFill(fill_type=None)
    failed_count = 0

    for r in range(2, ws_final.max_row + 1):
        for c in range(1, ws_checks.max_column + 1):
            if c in (6, 7):
                continue
            val = ws_values.cell(r, c).value
            if val in (True, "TRUE", "true", 1):
                ws_checks.cell(r, c).fill = clear_fill
            else:
                ws_checks.cell(r, c).fill = highlight
                failed_count += 1

    wb_ae.save(ae_path)
    wb_ae.close()
    wb_val.close()
    return failed_count == 0, failed_count


def append_meta_and_checklist(review_file, meta_rows, checklist_rows):
    wb = load_workbook(review_file)

    if "Automation_Meta" in wb.sheetnames:
        del wb["Automation_Meta"]
    if "Automation_Checklist" in wb.sheetnames:
        del wb["Automation_Checklist"]

    ws_meta = wb.create_sheet("Automation_Meta")
    ws_meta.cell(1, 1, "key")
    ws_meta.cell(1, 2, "value")
    for idx, (k, v) in enumerate(meta_rows.items(), start=2):
        ws_meta.cell(idx, 1, k)
        ws_meta.cell(idx, 2, v)

    ws_check = wb.create_sheet("Automation_Checklist")
    headers = ["check_name", "status", "failed_count", "details"]
    for c, h in enumerate(headers, start=1):
        ws_check.cell(1, c, h)
    for r, row in enumerate(checklist_rows, start=2):
        ws_check.cell(r, 1, row.get("check_name", ""))
        ws_check.cell(r, 2, row.get("status", ""))
        ws_check.cell(r, 3, row.get("failed_count", 0))
        ws_check.cell(r, 4, row.get("details", ""))

    wb.save(review_file)
    wb.close()


def read_meta(review_file):
    wb = load_workbook(review_file, data_only=True)
    if "Automation_Meta" not in wb.sheetnames:
        wb.close()
        return {}
    ws = wb["Automation_Meta"]
    meta = {}
    for r in range(2, ws.max_row + 1):
        k = ws.cell(r, 1).value
        v = ws.cell(r, 2).value
        if k:
            meta[str(k)] = "" if v is None else str(v)
    wb.close()
    return meta


def write_conso_to_ae_template(conso_df, ae_template_file, review_file):
    shutil.copyfile(ae_template_file, review_file)
    subprocess.run(["attrib", "-R", review_file], shell=True)

    wb = load_workbook(review_file)
    final = next(s for s in wb.sheetnames if s.lower() == "final")
    vlookup = next(s for s in wb.sheetnames if s.lower() == "vlookup")

    ws_final = wb[final]
    ws_vlookup = wb[vlookup]

    ws_final.delete_rows(1, ws_final.max_row)

    for c, col in enumerate(conso_df.columns, 1):
        ws_final.cell(1, c, col)

    for r, (_, row) in enumerate(conso_df.iterrows(), 2):
        for c, val in enumerate(row, 1):
            ws_final.cell(r, c, val)

    for c in range(17, 22):
        ws_final.cell(1, c, ws_vlookup.cell(1, c).value)
        base = ws_vlookup.cell(2, c).value
        for r in range(2, ws_final.max_row + 1):
            ws_final.cell(
                r,
                c,
                re.sub(r"(\$?[A-Z]+)2", lambda m: f"{m.group(1)}{r}", base)
                if isinstance(base, str) and base.startswith("=")
                else (base or ""),
            )

    wb.save(review_file)
    wb.close()


def create_review_from_conso(conso_df, review_file):
    with pd.ExcelWriter(review_file, engine="openpyxl") as writer:
        conso_df.to_excel(writer, sheet_name="Final", index=False)


def prepare_mode(args):
    conso_filename = os.path.basename(args.conso_file)
    final_name_template, ae_flag = resolve_ae_naming(conso_filename, args.reference_excel)
    forced_template_path = getattr(args, "ae_template_file", "") or ""
    forced_template_path = forced_template_path.strip()

    conso_df, checklist_rows, conso_has_fail = run_conso_check_once(args.conso_file)

    os.makedirs(args.output_dir, exist_ok=True)
    review_file = args.review_file or os.path.join(
        args.output_dir, f"{os.path.splitext(conso_filename)[0]}_review.xlsx"
    )

    ae_failed_cells = 0
    if ae_flag.lower() == "no":
        checklist_rows.append(
            {
                "check_name": "ae_template_check",
                "status": "PASS",
                "failed_count": 0,
                "details": "AE check not applicable for this file. Review file is not required.",
            }
        )
    else:
        if forced_template_path:
            ae_template_file = forced_template_path
            ae_flag = os.path.basename(forced_template_path)
        else:
            ae_template_file = os.path.join(args.ae_check_folder, ae_flag)
        if not os.path.exists(ae_template_file):
            raise FileNotFoundError(f"AE template not found: {ae_template_file}")

        write_conso_to_ae_template(conso_df, ae_template_file, review_file)
        values_file = create_values_file(review_file)
        clear_brand_division_zeros(review_file, values_file)
        ae_pass, ae_failed_cells = validate_checks_using_values(review_file, values_file)
        try:
            os.remove(values_file)
        except OSError:
            pass

        checklist_rows.append(
            {
                "check_name": "ae_formula_checks",
                "status": "PASS" if ae_pass else "FAIL",
                "failed_count": ae_failed_cells,
                "details": "Correct highlighted cells in Checks sheet and re-upload this review file.",
            }
        )

    overall_fail = conso_has_fail or ae_failed_cells > 0
    if ae_flag.lower() != "no":
        meta_rows = {
            "mode": "prepare",
            "conso_filename": conso_filename,
            "final_name_template": final_name_template,
            "ae_check_file_name": ae_flag,
            "prepared_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "overall_status": "FAIL" if overall_fail else "PASS",
            "next_step": (
                "Checks passed: output files are generated."
                if not overall_fail
                else "Do changes in highlighted fields and re-upload for validation."
            ),
        }
        append_meta_and_checklist(review_file, meta_rows, checklist_rows)
        print(f"REVIEW_FILE={review_file}")

    print(f"OVERALL_STATUS={'FAIL' if overall_fail else 'PASS'}")

    if overall_fail:
        if ae_flag.lower() == "no":
            # For web/back-end flows, emit a checklist workbook on failure.
            create_review_from_conso(conso_df, review_file)
            meta_rows = {
                "mode": "prepare",
                "conso_filename": conso_filename,
                "final_name_template": final_name_template,
                "ae_check_file_name": ae_flag,
                "prepared_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "overall_status": "FAIL",
                "next_step": "Fix source conso file and rerun.",
            }
            append_meta_and_checklist(review_file, meta_rows, checklist_rows)
            print(f"REVIEW_FILE={review_file}")
            print("Validation failed. Fix source conso file and run prepare again.")
            return {
                "status": "FAIL",
                "review_file": review_file,
                "final_tsv": "",
                "final_xlsx": "",
            }
        else:
            print("Review file has failed checks. Do changes and re-upload for validation.")
            return {
                "status": "FAIL",
                "review_file": review_file,
                "final_tsv": "",
                "final_xlsx": "",
            }

    if ae_flag.lower() == "no":
        final_df = conso_df.copy()
    else:
        values_file = create_values_file(review_file)
        final_sheet = get_final_sheet_name(values_file)
        final_df = pd.read_excel(values_file, sheet_name=final_sheet, dtype=str, keep_default_na=False)
        try:
            os.remove(values_file)
        except OSError:
            pass

    if {"brand_name", "exposed_sku"}.issubset(final_df.columns):
        final_df.loc[final_df["brand_name"] == "No Brand", "exposed_sku"] = "NA"

    out_tsv, out_xlsx = write_final_outputs(final_df, final_name_template, args.output_dir)
    print(f"FINAL_TSV={out_tsv}")
    print(f"FINAL_XLSX={out_xlsx}")
    if ae_flag.lower() == "no":
        print("No review file required. Output files generated with today's date.")
    else:
        print("Review checks passed. Review file and output files generated with today's date.")
    return {
        "status": "PASS",
        "review_file": review_file if ae_flag.lower() != "no" else "",
        "final_tsv": out_tsv,
        "final_xlsx": out_xlsx,
    }


def infer_final_name(template):
    if re.search(r"\d{8}", template):
        return re.sub(r"\d{8}", today_compact(), template)
    return f"{template}_{today_compact()}"


def write_final_outputs(final_df, template, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    final_name = infer_final_name(template)
    out_tsv = os.path.join(output_dir, final_name + ".tsv")
    out_xlsx = os.path.join(output_dir, final_name + ".xlsx")

    final_df.to_csv(out_tsv, sep="\t", index=False, encoding="utf-8")
    final_df.to_excel(out_xlsx, index=False)
    return out_tsv, out_xlsx


def get_final_sheet_name(path):
    wb = load_workbook(path, data_only=True)
    try:
        return next(s for s in wb.sheetnames if s.lower() == "final")
    finally:
        wb.close()


def finalize_mode(args):
    meta = read_meta(args.review_file)
    template = args.final_name_template or meta.get("final_name_template", "").strip()
    if not template:
        raise ValueError(
            "final_name_template not found in Automation_Meta sheet. "
            "Pass --final-name-template explicitly."
        )

    ae_flag = meta.get("ae_check_file_name", "no").strip()

    if ae_flag.lower() != "no":
        values_file = create_values_file(args.review_file)
        clear_brand_division_zeros(args.review_file, values_file)
        ae_pass, ae_failed_cells = validate_checks_using_values(args.review_file, values_file)
        if not ae_pass:
            try:
                os.remove(values_file)
            except OSError:
                pass
            raise ValueError(
                f"File is still not corrected for highlighted field. Failed cells: {ae_failed_cells}. "
                "Please do changes and re-upload for validation."
            )
        final_sheet = get_final_sheet_name(values_file)
        final_df = pd.read_excel(values_file, sheet_name=final_sheet, dtype=str, keep_default_na=False)
        try:
            os.remove(values_file)
        except OSError:
            pass
    else:
        final_sheet = get_final_sheet_name(args.review_file)
        final_df = pd.read_excel(args.review_file, sheet_name=final_sheet, dtype=str, keep_default_na=False)

    if {"brand_name", "exposed_sku"}.issubset(final_df.columns):
        final_df.loc[final_df["brand_name"] == "No Brand", "exposed_sku"] = "NA"

    out_tsv, out_xlsx = write_final_outputs(final_df, template, args.output_dir)

    print(f"FINAL_TSV={out_tsv}")
    print(f"FINAL_XLSX={out_xlsx}")
    print("Validation passed. Output files generated with today's date.")
    return {
        "status": "PASS",
        "review_file": args.review_file,
        "final_tsv": out_tsv,
        "final_xlsx": out_xlsx,
    }


def _is_excel_file(path):
    return str(path).lower().endswith((".xlsx", ".xls", ".xlsm", ".xltx", ".xltm"))


def _copy_if_needed(src, dst):
    if not src:
        return
    if os.path.abspath(src) == os.path.abspath(dst):
        return
    shutil.copyfile(src, dst)


def backend_mode(op_file, ip_file, master_file, output_file, ae_template_file=""):
    """
    Back-end compatibility mode:
    python PP_conso_check 1.py <op_file> <ip_file> <master_file> <output_xlsx>
    - Fresh run: uses OP + master reference mapping.
    - Revalidation run: if IP points to Excel review file, validate IP and generate finals.
    """
    output_dir = os.path.dirname(os.path.abspath(output_file)) or "."
    os.makedirs(output_dir, exist_ok=True)

    op_exists = bool(op_file) and os.path.exists(op_file)
    ip_exists = bool(ip_file) and os.path.exists(ip_file)

    # Platform rule:
    # - Fresh run if source file exists.
    # - Revalidation only when source is absent and review workbook is provided.
    if (not op_exists) and ip_exists and _is_excel_file(ip_file):
        print("MODE=REVALIDATION")
        result = finalize_mode(
            argparse.Namespace(
                review_file=ip_file,
                output_dir=output_dir,
                final_name_template="",
            )
        )
        _copy_if_needed(result.get("final_xlsx", ""), output_file)
        return

    print("MODE=FRESH")
    if not op_exists:
        raise FileNotFoundError(
            f"Fresh mode requires source file, but not found: '{op_file}'. "
            "Provide source file for fresh run or review workbook for revalidation."
        )
    ae_template_arg = (ae_template_file or "").strip()
    result = prepare_mode(
        argparse.Namespace(
            conso_file=op_file,
            output_dir=output_dir,
            review_file=output_file,
            reference_excel=master_file if master_file else REFERENCE_EXCEL,
            ae_check_folder=AE_CHECK_FOLDER,
            ae_template_file=ae_template_arg,
        )
    )
    if not result:
        return

    if result.get("status") == "PASS" and result.get("final_xlsx"):
        _copy_if_needed(result["final_xlsx"], output_file)
    elif result.get("review_file"):
        _copy_if_needed(result["review_file"], output_file)


def build_parser():
    parser = argparse.ArgumentParser(
        description="PP conso web-compatible checklist flow (prepare -> finalize)."
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    p_prepare = sub.add_parser("prepare", help="Generate review/highlight file and checklist.")
    p_prepare.add_argument("--conso-file", required=True, help="Path to source conso TSV file.")
    p_prepare.add_argument("--output-dir", required=True, help="Directory for review file.")
    p_prepare.add_argument("--review-file", default="", help="Optional explicit review file path.")
    p_prepare.add_argument(
        "--reference-excel",
        default=REFERENCE_EXCEL,
        help="Reference Excel containing conso_file/final_name_template/AE_check_file_name.",
    )
    p_prepare.add_argument("--ae-check-folder", default=AE_CHECK_FOLDER, help="Folder with AE templates.")
    p_prepare.add_argument(
        "--ae-template-file",
        default="",
        help="Optional AE template file path to override template lookup from reference.",
    )

    p_finalize = sub.add_parser("finalize", help="Validate corrected review file and generate final outputs.")
    p_finalize.add_argument("--review-file", required=True, help="Corrected review file uploaded from UI.")
    p_finalize.add_argument("--output-dir", required=True, help="Directory for generated final files.")
    p_finalize.add_argument(
        "--final-name-template",
        default="",
        help="Optional fallback final_name_template if Automation_Meta sheet is missing.",
    )

    return parser


def main():
    # Back-end compatibility path used by worker-style integrations.
    if len(sys.argv) in {5, 6} and sys.argv[1] not in {"prepare", "finalize"}:
        if len(sys.argv) == 5:
            _, op_file, ip_file, master_file, output_file = sys.argv
            ae_template_file = ""
        else:
            _, op_file, ip_file, master_file, output_file, ae_template_file = sys.argv
        backend_mode(op_file, ip_file, master_file, output_file, ae_template_file)
        return

    parser = build_parser()
    args = parser.parse_args()

    if args.mode == "prepare":
        prepare_mode(args)
    elif args.mode == "finalize":
        finalize_mode(args)
    else:
        parser.error("Unknown mode")


if __name__ == "__main__":
    main()
