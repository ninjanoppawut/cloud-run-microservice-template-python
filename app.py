# app.py (drop-in replacement for the template’s app.py)
import os
from flask import Flask, request, jsonify
from google.cloud.run_v2 import JobsClient

PROJECT_ID = os.getenv("PROJECT_ID")
REGION     = os.getenv("REGION", "asia-southeast1")
JOB_NAME   = os.getenv("JOB_NAME", "golf-analyzer-job")

app = Flask(__name__)
jobs = JobsClient()

def job_res(pid, region, name):
    return f"projects/{pid}/locations/{region}/jobs/{name}"

@app.get("/healthz")
def healthz(): return "ok", 200

@app.post("/run")
def run():
    try:
        if not PROJECT_ID:
            return jsonify({"error":"PROJECT_ID env missing"}), 500
        payload = request.get_json(silent=True) or {}
        if "args" in payload and isinstance(payload["args"], list):
            args = payload["args"]
        else:
            src = payload.get("source")
            if not src or not str(src).startswith("gs://"):
                return jsonify({"error":"missing or invalid 'source' (gs://...)"}), 400
            args = ["--source", str(src)]
        op = jobs.run_job(
            name=job_res(PROJECT_ID, REGION, JOB_NAME),
            overrides={"container_overrides":[{"args": args}]}
        )
        exec_name = getattr(getattr(op, "metadata", None), "name", None) or op.operation.name
        return jsonify({"status":"accepted","job":JOB_NAME,"execution":exec_name}), 202
    except Exception as e:
        return jsonify({"error": str(e)}), 500
