from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime

import subprocess
import tempfile
import os
import shutil
import signal

from pdf_report_generator import generate_pdf_from_ai_report
from ai_analyzer import analyze_output_with_gemini


# =========================================================
# ENV
# =========================================================

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("SUPABASE_URL or SUPABASE_SERVICE_KEY missing in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# =========================================================
# APP
# =========================================================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "https://pl-conso-frontend.vercel.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


BASE_WORKDIR = "./work"
os.makedirs(BASE_WORKDIR, exist_ok=True)


# =========================================================
# HELPERS
# =========================================================

def log(run_id: str, level: str, message: str):
    """
    Insert one log row into DB
    """
    if not message or message.strip() == "":
        return

    supabase.table("run_logs").insert({
        "run_id": run_id,
        "level": level,
        "message": message.strip()
    }).execute()


def download_from_storage(bucket, storage_path, local_path):
    data = supabase.storage.from_(bucket).download(storage_path)
    with open(local_path, "wb") as f:
        f.write(data)


def upload_to_storage(bucket, storage_path, local_path):
    with open(local_path, "rb") as f:
        res = supabase.storage.from_(bucket).upload(storage_path, f)

    print("UPLOAD RESULT:", res)   # 🔥 add this



# =========================================================
# MAIN EXECUTION
# =========================================================

def execute_run(run_id: str):

    run_dir = os.path.join(BASE_WORKDIR, f"run_{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    try:
        # ---------------------------------------
        # 1. Mark running
        # ---------------------------------------
        supabase.table("runs").update({
            "status": "running",
            "start_time": datetime.utcnow().isoformat()
        }).eq("id", run_id).execute()

        run = (
            supabase.table("runs")
            .select("*")
            .eq("id", run_id)
            .single()
            .execute()
            .data
        )

        log(run_id, "INFO", "Run started")

        op_path = run["op_filename"]
        ip_path = run["ip_filename"]
        master_path = run["master_filename"]

        if not op_path or not ip_path or not master_path:
            raise Exception("Missing input files in DB")

        # ---------------------------------------
        # 2. Prepare paths
        # ---------------------------------------
        op_local = os.path.join(run_dir, os.path.basename(op_path))
        ip_local = os.path.join(run_dir, os.path.basename(ip_path))
        master_local = os.path.join(run_dir, os.path.basename(master_path))
        output_local = os.path.join(run_dir, "output.xlsx")

        log(run_id, "INFO", f"OP file: {op_path}")
        log(run_id, "INFO", f"IP file: {ip_path}")
        log(run_id, "INFO", f"MASTER file: {master_path}")

        # ---------------------------------------
        # 3. Download
        # ---------------------------------------
        log(run_id, "INFO", "Downloading input files")

        download_from_storage("input-files", op_path, op_local)
        download_from_storage("crawl-input", ip_path, ip_local)
        download_from_storage("masters", master_path, master_local)

        log(run_id, "INFO", "All files downloaded successfully")

        # ---------------------------------------
        # 4. Run script (REALTIME LOGGING)
        # ---------------------------------------
        log(run_id, "INFO", "Starting Python script")

        process = subprocess.Popen(
            [
                "python",
                "-u",                     # 🔥 VERY IMPORTANT → unbuffered
                "pl_conso_check.py",
                op_local,
                ip_local,
                master_local,
                output_local,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        supabase.table("runs").update({
    "process_pid": process.pid
}).eq("id", run_id).execute()


        # 🔥 STREAM LIVE LOGS
        for line in iter(process.stdout.readline, ''):
            log(run_id, "INFO", line.rstrip())

        process.stdout.close()
        process.wait()
        if process.returncode == -9:
            log(run_id, "INFO", "Run cancelled by user")
            return   # DO NOT go to except



        if process.returncode != 0:
            raise Exception("Script failed")

        log(run_id, "INFO", "Script finished successfully")

        # ---------------------------------------
        # 5. AI analysis
        # ---------------------------------------
        try:
            log(run_id, "INFO", "Sending output to AI")

            ai_report = analyze_output_with_gemini(output_local)

            supabase.table("run_ai_reports").insert({
                "run_id": run_id,
                "report_json": ai_report,
                "summary": ai_report.get("summary"),
                "accuracy": ai_report.get("accuracy"),
                "verdict": ai_report.get("verdict")
            }).execute()

            log(run_id, "INFO", "AI report generated")

        except Exception as e:
            log(run_id, "ERROR", f"AI failed: {str(e)}")

        # ---------------------------------------
        # 6. Upload result
        # ---------------------------------------
        filename = f"{run['run_uuid']}.xlsx"

        upload_to_storage("run-outputs", filename, output_local)

        supabase.table("run_files").insert({
            "run_id": run_id,
            "filename": filename,
            "file_type": "FINAL_OUTPUT",
            "storage_path": filename
        }).execute()

        # -------------------------
        # 8. complete
        # -------------------------
        supabase.table("runs").update({
            "status": "completed",
            "end_time": datetime.utcnow().isoformat(),
            "process_pid": None
        }).eq("id", run_id).execute()

    except Exception as e:
        log(run_id, "ERROR", str(e))

        current = (
            supabase.table("runs")
            .select("status")
            .eq("id", run_id)
            .single()
            .execute()
            .data
        )

        # never override cancelled
        if not current or current["status"] != "cancelled":
            supabase.table("runs").update({
                "status": "failed",
                "end_time": datetime.utcnow().isoformat(),
                "process_pid": None
            }).eq("id", run_id).execute()

    finally:
        shutil.rmtree(run_dir, ignore_errors=True)
        
def execute_input_run(run_id: str):

    run_dir = os.path.join(BASE_WORKDIR, f"input_run_{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    try:
        # -----------------------------
        # 1. mark running
        # -----------------------------
        supabase.table("runs").update({
            "status": "running",
            "start_time": datetime.utcnow().isoformat()
        }).eq("id", run_id).execute()

        run = (
            supabase.table("runs")
            .select("*")
            .eq("id", run_id)
            .single()
            .execute()
            .data
        )

        log(run_id, "INFO", "Input creation started")

        # -----------------------------
        # 2. local paths
        # -----------------------------
        biz_local = os.path.join(run_dir, "biz.xlsx")
        crawl_local = os.path.join(run_dir, "crawl.xlsx")
        template_local = os.path.join(run_dir, "template.xlsx")
        output_local = os.path.join(run_dir, "merged_output.xlsx")
        zip_local = os.path.join(run_dir, "all_tsv_files.zip")


        # -----------------------------
        # 3. download files
        # -----------------------------
        download_from_storage(
            "input-creation-bussiness-file",
            run["op_filename"],
            biz_local
        )

        download_from_storage(
            "input-creation-crawl-team-file",
            run["ip_filename"],
            crawl_local
        )

        download_from_storage(
            "input-creation-final-template",
            run["master_filename"],
            template_local
        )

        log(run_id, "INFO", "All files downloaded")

        # -----------------------------
        # 4. run python script
        # -----------------------------
        process = subprocess.Popen(
            [
                "python",
                "-u",
                "input_creation_script.py",
                biz_local,
                crawl_local,
                template_local,
                output_local
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        supabase.table("runs").update({
    "process_pid": process.pid
}).eq("id", run_id).execute()


        for line in process.stdout:
            log(run_id, "INFO", line.rstrip())

        process.wait()

        # 🔥 CANCELLED PROCESS (killed)
        if process.returncode == -9:
            log(run_id, "INFO", "Run cancelled by user")
            return

# real failure
        if process.returncode != 0:
             raise Exception("Script failed")


        # -----------------------------
        # 5. upload result
        # -----------------------------
        output_filename = f"{run['run_uuid']}.xlsx"

        upload_to_storage(
            "input-creation-output",
            output_filename,
            output_local
        )

        supabase.table("run_files").insert({
            "run_id": run_id,
            "filename": output_filename,
            "file_type": "MERGED_OUTPUT",
            "storage_path": output_filename
        }).execute()

        # -----------------------------
        # Upload TSV ZIP (ADD THIS)
        # -----------------------------
        zip_filename = f"{run['run_uuid']}_tsv.zip"

        upload_to_storage(
            "input-creation-output",
             zip_filename,
             zip_local
        )

        supabase.table("run_files").insert({
            "run_id": run_id,
            "filename": zip_filename,
            "file_type": "TSV_ZIP",
            "storage_path": zip_filename
        }).execute()


        # -----------------------------
        # 6. mark completed
        # -----------------------------
        supabase.table("runs").update({
            "status": "completed",
            "end_time": datetime.utcnow().isoformat()
        }).eq("id", run_id).execute()

        log(run_id, "INFO", "Input creation completed")

        except Exception as e:
        log(run_id, "ERROR", str(e))

        current = (
            supabase.table("runs")
            .select("status")
            .eq("id", run_id)
            .single()
            .execute()
            .data
        )

        if current and current["status"] == "cancelled":
            return

        supabase.table("runs").update({
            "status": "failed",
            "end_time": datetime.utcnow().isoformat()
        }).eq("id", run_id).execute()

    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


# =========================================================
# API
# =========================================================

@app.post("/run/{run_id}")
def start_run(run_id: str, bg: BackgroundTasks):
    bg.add_task(execute_run, run_id)
    return {"status": "started"}


@app.post("/run/{run_id}/rerun")
def rerun(run_id: str, bg: BackgroundTasks):

    old = (
        supabase.table("runs")
        .select("*")
        .eq("id", run_id)
        .single()
        .execute()
        .data
    )

    new = (
        supabase.table("runs")
        .insert({
            "user_id": old["user_id"],
            "project_id": old["project_id"],
            "site_id": old["site_id"],
            "scope": old["scope"],
            "op_filename": old["op_filename"],
            "ip_filename": old["ip_filename"],
            "master_filename": old["master_filename"],
            "status": "pending",
             "automation_slug": old["automation_slug"]
        })
        .execute()
        .data[0]
    )

    bg.add_task(execute_run, new["id"])

    return {"status": "rerun_started"}


@app.get("/run/{run_id}/ai-report")
def get_ai_report(run_id: str):
    return (
        supabase.table("run_ai_reports")
        .select("*")
        .eq("run_id", run_id)
        .single()
        .execute()
        .data
    )


@app.get("/run/{run_id}/ai-report-pdf")
def download_ai_report_pdf(run_id: str):

    row = (
        supabase.table("run_ai_reports")
        .select("*")
        .eq("run_id", run_id)
        .single()
        .execute()
        .data
    )

    tmp = f"/tmp/{run_id}.pdf"

    generate_pdf_from_ai_report(row["report_json"], tmp)

    return FileResponse(tmp, media_type="application/pdf")

@app.post("/input-run/{run_id}")
def start_input_run(run_id: str, bg: BackgroundTasks):
    bg.add_task(execute_input_run, run_id)
    return {"status": "started"}

@app.post("/input-run/{run_id}/rerun")
def rerun_input(run_id: str, bg: BackgroundTasks):

    # get old run
    old = (
        supabase.table("runs")
        .select("*")
        .eq("id", run_id)
        .single()
        .execute()
        .data
    )

    # create new run row
    new = (
        supabase.table("runs")
        .insert({
            "user_id": old["user_id"],
            "project_id": old["project_id"],
            "site_id": old["site_id"],
            "scope": old["scope"],
            "op_filename": old["op_filename"],
            "ip_filename": old["ip_filename"],
            "master_filename": old["master_filename"],
            "status": "pending",
            "automation_slug": old["automation_slug"]

        })
        .execute()
        .data[0]
    )

    # start INPUT execution
    bg.add_task(execute_input_run, new["id"])

    return {"status": "input_rerun_started"}

# =========================================================
# UNIVERSAL DELETE RUN (PL CONSO + PL INPUT)
# =========================================================


@app.post("/runs/{run_id}/cancel")
def cancel_run(run_id: str):

    run = (
        supabase.table("runs")
        .select("*")
        .eq("id", run_id)
        .single()
        .execute()
        .data
    )

    if not run:
        return {"error": "Run not found"}

    # -------------------------
    # 1. HARD KILL PROCESS
    # -------------------------
    pid = run.get("process_pid")

    if pid:
        try:
            os.kill(pid, signal.SIGKILL)
        except:
            pass

    # -------------------------
    # 2. DELETE STORAGE FILES
    # -------------------------
    files = (
        supabase.table("run_files")
        .select("*")
        .eq("run_id", run_id)
        .execute()
        .data
    )

    for f in files or []:
        try:
            supabase.storage.from_("run-outputs").remove([f["storage_path"]])
        except:
            pass

    # -------------------------
    # 3. CLEAN CHILD TABLES
    # -------------------------
    supabase.table("run_logs").delete().eq("run_id", run_id).execute()
    supabase.table("run_files").delete().eq("run_id", run_id).execute()
    supabase.table("run_ai_reports").delete().eq("run_id", run_id).execute()

    # -------------------------
    # 4. UPDATE STATUS ONLY
    # -------------------------
    supabase.table("runs").update({
        "status": "cancelled",
        "end_time": datetime.utcnow().isoformat(),
        "process_pid": None
    }).eq("id", run_id).execute()

    return {"status": "cancelled"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


