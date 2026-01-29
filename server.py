from fastapi import FastAPI, BackgroundTasks
from pdf_report_generator import generate_pdf_from_ai_report
import tempfile
from fastapi.responses import FileResponse
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime
import subprocess
import os
import shutil
from fastapi.middleware.cors import CORSMiddleware
from ai_analyzer import analyze_output_with_gemini

# ------------------ Load ENV ------------------

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("SUPABASE_URL or SUPABASE_SERVICE_KEY missing in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ------------------ App ------------------

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_WORKDIR = "./work"
os.makedirs(BASE_WORKDIR, exist_ok=True)

# ------------------ Helpers ------------------

def log(run_id, level, message):
    supabase.table("run_logs").insert({
        "run_id": run_id,
        "level": level,
        "message": message
    }).execute()

def download_from_storage(bucket, storage_path, local_path):
    data = supabase.storage.from_(bucket).download(storage_path)
    with open(local_path, "wb") as f:
        f.write(data)

def upload_to_storage(bucket, storage_path, local_path):
    with open(local_path, "rb") as f:
        supabase.storage.from_(bucket).upload(storage_path, f)

# ------------------ Main Runner ------------------

def execute_run(run_id: str):
    run_dir = os.path.join(BASE_WORKDIR, f"run_{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    try:
        # 1. Mark running
        supabase.table("runs").update({
            "status": "running",
            "start_time": datetime.utcnow().isoformat()
        }).eq("id", run_id).execute()

        run = supabase.table("runs").select("*").eq("id", run_id).single().execute().data

        log(run_id, "INFO", "Run started")

        # 2. Read file paths from DB
        op_path = run["op_filename"]
        ip_path = run["ip_filename"]
        master_path = run["master_filename"]

        if not op_path or not ip_path or not master_path:
            raise Exception("One or more input file paths are missing in DB")

        # 3. Prepare local paths
        op_local = os.path.join(run_dir, os.path.basename(op_path))
        ip_local = os.path.join(run_dir, os.path.basename(ip_path))
        master_local = os.path.join(run_dir, os.path.basename(master_path))
        output_local = os.path.join(run_dir, "output.xlsx")

        log(run_id, "INFO", f"OP file: {op_path}")
        log(run_id, "INFO", f"IP file: {ip_path}")
        log(run_id, "INFO", f"MASTER file: {master_path}")

        # 4. Download files
        log(run_id, "INFO", "Downloading input files")

        download_from_storage("input-files", op_path, op_local)
        download_from_storage("crawl-input", ip_path, ip_local)
        download_from_storage("masters", master_path, master_local)

        log(run_id, "INFO", "All files downloaded successfully")

        # 5. Run script
        log(run_id, "INFO", "Starting Python script")

        result = subprocess.run(
            ["python", "pl_conso_check.py", op_local, ip_local, master_local, output_local],
            capture_output=True,
            text=True
        )

        if result.stdout:
            log(run_id, "INFO", result.stdout)

        if result.returncode != 0:
            raise Exception(result.stderr)

        log(run_id, "INFO", "Script finished successfully")

        # 6. AI Analysis
        try:
            log(run_id, "INFO", "Sending output to LLM for AI analysis")

            ai_report = analyze_output_with_gemini(output_local)

            supabase.table("run_ai_reports").insert({
                "run_id": run_id,
                "report_json": ai_report,
                "summary": ai_report.get("summary"),
                "accuracy": ai_report.get("accuracy"),
                "verdict": ai_report.get("verdict")
            }).execute()

            log(run_id, "INFO", "AI report generated and saved")

        except Exception as e:
            log(run_id, "ERROR", f"AI analysis failed: {str(e)}")

        # 7. Upload output
        output_filename = f"{run['run_uuid']}.xlsx"
        storage_path = output_filename

        log(run_id, "INFO", "Uploading output to run-outputs bucket")

        upload_to_storage("run-outputs", storage_path, output_local)

        # 8. Insert into run_files table
        supabase.table("run_files").insert({
            "run_id": run_id,
            "filename": output_filename,
            "file_type": "FINAL_OUTPUT",
            "storage_path": storage_path
        }).execute()

        # 9. Mark completed
        supabase.table("runs").update({
            "status": "completed",
            "end_time": datetime.utcnow().isoformat()
        }).eq("id", run_id).execute()

        log(run_id, "INFO", "Run completed successfully")

    except Exception as e:
        log(run_id, "ERROR", str(e))

        supabase.table("runs").update({
            "status": "failed",
            "end_time": datetime.utcnow().isoformat()
        }).eq("id", run_id).execute()

    finally:
        shutil.rmtree(run_dir, ignore_errors=True)

# ------------------ API ------------------

@app.post("/run/{run_id}")
def start_run(run_id: str, bg: BackgroundTasks):
    bg.add_task(execute_run, run_id)
    return {"status": "started"}

@app.post("/run/{run_id}/rerun")
def rerun(run_id: str, bg: BackgroundTasks):
    # 1. Load old run
    old_res = supabase.table("runs").select("*").eq("id", run_id).execute()

    if not old_res.data:
        return {"error": "Run not found"}

    old = old_res.data[0]

    # 2. Create new run with SAME CONFIG
    insert_res = supabase.table("runs").insert({
        "user_id": old["user_id"],
        "project_id": old["project_id"],
        "site_id": old["site_id"],
        "scope": old["scope"],
        "op_filename": old["op_filename"],
        "ip_filename": old["ip_filename"],
        "master_filename": old["master_filename"],
        "status": "pending"
    }).execute()

    new = insert_res.data[0]

    # 3. Execute new run
    bg.add_task(execute_run, new["id"])

    return {
        "status": "rerun_started",
        "old_run_id": run_id,
        "new_run_id": new["id"]
    }

@app.get("/run/{run_id}/ai-report")
def get_ai_report(run_id: str):
    data = supabase.table("run_ai_reports").select("*").eq("run_id", run_id).single().execute().data
    return data

@app.get("/run/{run_id}/ai-report-pdf")
def download_ai_report_pdf(run_id: str):
    row = supabase.table("run_ai_reports").select("*").eq("run_id", run_id).single().execute().data

    if not row:
        return {"error": "AI report not found"}

    ai_report = row["report_json"]

    tmp_path = f"/tmp/ai_report_{run_id}.pdf"

    generate_pdf_from_ai_report(ai_report, tmp_path)

    return FileResponse(
        tmp_path,
        media_type="application/pdf",
        filename=f"AI_Report_{run_id}.pdf"
    )

@app.get("/admin/runs-with-ai")
def get_runs_with_ai():
    res = supabase.table("runs").select("""
        id,
        run_uuid,
        status,
        created_at,
        run_ai_reports (
            verdict,
            accuracy,
            summary,
            report_json
        )
    """).execute()

    return res.data

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
