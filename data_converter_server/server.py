from flask import Flask, send_from_directory, request, jsonify, send_file, Response, stream_with_context
import os
import uuid
import zipfile
import threading
import queue
import logging
import json
import time

from etl_runner import run_etl_job

# server.py lives in ventures-sorting/data_converter_server/
# repo root is one directory up
SERVER_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SERVER_DIR)

UI_DIR = os.path.join(REPO_ROOT, "data_converter_ui")

app = Flask(
    __name__,
    static_folder=UI_DIR,
    static_url_path=""
)

# -------------------------
# Helpers
# -------------------------
def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def make_zip_from_dir(src_dir: str, zip_path: str) -> int:
    """
    Zips the contents of src_dir into zip_path.
    Returns the number of files added.
    """
    file_count = 0
    if not os.path.isdir(src_dir):
        return 0

    # Recreate zip fresh each time
    if os.path.exists(zip_path):
        os.remove(zip_path)

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(src_dir):
            for fname in files:
                abs_path = os.path.join(root, fname)
                rel_path = os.path.relpath(abs_path, src_dir)  # path inside zip
                zf.write(abs_path, arcname=rel_path)
                file_count += 1

    return file_count

class QueueHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        try:
            msg = self.format(record)
            self.log_queue.put({"type": "log", "message": msg, "level": record.levelname})
        except Exception:
            self.handleError(record)

def job_paths(job_id: str):
    runs_dir = os.path.join(REPO_ROOT, "runs")
    job_dir = os.path.join(runs_dir, job_id)
    input_dir = os.path.join(job_dir, "input")
    output_dir = os.path.join(job_dir, "output")
    zip_path = os.path.join(job_dir, "output.zip")
    return job_dir, input_dir, output_dir, zip_path

# -------------------------
# Static UI serving
# -------------------------
@app.route("/")
def serve_index():
    return send_from_directory(UI_DIR, "index.html")

@app.route("/<path:path>")
def serve_static(path):
    return send_from_directory(UI_DIR, path)

# -------------------------
# API: job status
# -------------------------
@app.route("/api/status/<job_id>", methods=["GET"])
def api_status(job_id):
    job_dir, input_dir, output_dir, zip_path = job_paths(job_id)
    exists = os.path.isdir(job_dir)
    zip_exists = os.path.isfile(zip_path)
    output_has_files = False

    if os.path.isdir(output_dir):
        for root, dirs, files in os.walk(output_dir):
            if files:
                output_has_files = True
                break

    return jsonify({
        "ok": True,
        "job_id": job_id,
        "exists": exists,
        "paths": {
            "job_dir": job_dir,
            "input_dir": input_dir,
            "output_dir": output_dir,
            "zip_path": zip_path
        },
        "artifacts": {
            "output_has_files": output_has_files,
            "zip_exists": zip_exists,
            "download_url": f"/api/download/{job_id}" if zip_exists else None
        }
    }), 200

# -------------------------
# API: download output zip
# -------------------------
@app.route("/api/download/<job_id>", methods=["GET"])
def api_download(job_id):
    job_dir, input_dir, output_dir, zip_path = job_paths(job_id)

    if not os.path.isdir(job_dir):
        return jsonify({"ok": False, "error": "job_not_found"}), 404

    if not os.path.isfile(zip_path):
        return jsonify({"ok": False, "error": "no_output_zip"}), 404

    # Give a friendly filename to the browser
    dl_name = f"{job_id}_output.zip"
    return send_file(zip_path, as_attachment=True, download_name=dl_name)

# -------------------------
# API: convert endpoint (stage + run + package)
# -------------------------
@app.route("/api/convert", methods=["POST"])
def api_convert():
    """
    Accepts multipart/form-data:
      - target: 'omop' | 'fhir'
      - files: one or more uploaded files

    Streams NDJSON:
    - Log events: {"type": "log", "message": "...", "level": "INFO"}
    - Final result: {"type": "result", "ok": true, ...}
    """
    target = (request.form.get("target", "omop") or "omop").strip().lower()
    if target not in ("omop", "fhir"):
        target = "omop"

    # Collect files
    uploaded_files = request.files.getlist("files")
    if not uploaded_files:
        uploaded_files = request.files.getlist("files[]")
    if not uploaded_files and len(request.files) > 0:
        all_files = []
        for k in request.files.keys():
            all_files.extend(request.files.getlist(k))
        uploaded_files = all_files

    job_id = str(uuid.uuid4())
    job_dir, input_dir, output_dir, zip_path = job_paths(job_id)

    ensure_dir(input_dir)
    ensure_dir(output_dir)

    def safe_filename(name: str) -> str:
        base = os.path.basename(name)
        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._- ")
        cleaned = "".join(ch for ch in base if ch in allowed).strip()
        return cleaned or "uploaded"

    allowed_exts = {".csv", ".xlsx", ".xls", ".json", ".sql"}

    saved = []
    skipped = []

    for f in uploaded_files:
        if not f or not getattr(f, "filename", None):
            continue

        original = f.filename
        fname = safe_filename(original)
        root, ext = os.path.splitext(fname)
        ext_lower = ext.lower()

        if ext_lower not in allowed_exts:
            skipped.append({"filename": original, "reason": f"unsupported_ext:{ext_lower or 'none'}"})
            continue

        dest_path = os.path.join(input_dir, fname)

        # Handle duplicates by appending _2, _3, ...
        if os.path.exists(dest_path):
            i = 2
            while True:
                candidate = os.path.join(input_dir, f"{root}_{i}{ext}")
                if not os.path.exists(candidate):
                    dest_path = candidate
                    fname = os.path.basename(candidate)
                    break
                i += 1

        f.save(dest_path)
        saved.append(fname)

    def generate():
        log_queue = queue.Queue()
        queue_handler = QueueHandler(log_queue)
        root_logger = logging.getLogger()
        root_logger.addHandler(queue_handler)
        # Capture all logs down to INFO (or lower if needed)
        # etl_runner sets level to INFO, we respect that or enforce it here.
        root_logger.setLevel(logging.INFO)

        result_container = {}

        def run_proc():
            try:
                # Run ETL (synchronous)
                exit_code, message = run_etl_job(
                    target=target,
                    data_dir=input_dir,
                    mapping_file=None,
                    output_dir=output_dir,
                )
                
                output_files_count = 0
                if exit_code == 0:
                    output_files_count = make_zip_from_dir(output_dir, zip_path)

                    if output_files_count == 0:
                        exit_code = 1
                        message = "ETL completed without errors but produced no output files. Verify uploaded IHID inputs."
                
                result_container.update({
                    "exit_code": exit_code,
                    "message": message,
                    "output_files_count": output_files_count
                })

            except Exception as e:
                logging.error(f"ETL fatal exception: {e}")
                result_container.update({
                    "exit_code": 1, 
                    "message": str(e),
                    "output_files_count": 0
                })
            finally:
                # Use a unique sentinel to stop
                log_queue.put(None)

        t = threading.Thread(target=run_proc)
        t.start()

        while True:
            item = log_queue.get()
            if item is None:
                break
            yield json.dumps(item) + "\n"
        
        t.join()
        root_logger.removeHandler(queue_handler)

        # Retrieve results
        exit_code = result_container.get("exit_code", 1)
        message = result_container.get("message", "Unknown error")
        output_files_count = result_container.get("output_files_count", 0)

        final_response = {
            "type": "result",
            "ok": exit_code == 0,
            "job_id": job_id,
            "status": "completed" if exit_code == 0 else "failed",
            "etl": {
                "exit_code": exit_code,
                "message": message
            },
            "received": {
                "target": target,
                "file_count": len(saved),
                "skipped_count": len(skipped),
                "saved_filenames_preview": saved[:10],
                "skipped_preview": skipped[:10],
            },
            "artifacts": {
                "output_files_count": output_files_count,
                "zip_exists": os.path.isfile(zip_path),
                "download_url": f"/api/download/{job_id}" if os.path.isfile(zip_path) else None
            },
            "paths": {
                "job_dir": job_dir,
                "input_dir": input_dir,
                "output_dir": output_dir,
            }
        }
        yield json.dumps(final_response) + "\n"

    return Response(stream_with_context(generate()), mimetype='application/x-ndjson')


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
