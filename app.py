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

@app.get("/")
def index():
    return jsonify({"ok": True, "hint": 'POST /run with {"source":"gs://..."}'}), 200

@app.get("/healthz")
def healthz():
    return "ok", 200

@app.post("/")
def root_compat():
    """
    Accepts your curl:
      curl -X POST "$SVC_URL" \
        -H "Authorization: bearer $(gcloud auth print-identity-token)" \
        -H "Content-Type: application/json" \
        -d '{"name":"Developer"}'
    - If only 'name' is provided, reply 200 with a friendly message.
    - If 'source' (gs://...) or explicit 'args' are present, trigger the Job.
    """
    payload = request.get_json(silent=True) or {}

    # 1) simple ACK when body has just {"name": "..."}
    if "source" not in payload and "args" not in payload:
        name = payload.get("name", "there")
        return jsonify({
            "ok": True,
            "message": f"Hello, {name}! To start processing, POST /run with {{\"source\":\"gs://...\"}}.",
            "received": payload
        }), 200

    # 2) optional: allow triggering the job from "/" too
    try:
        if not PROJECT_ID:
            return jsonify({"error": "PROJECT_ID env missing"}), 500

        if "args" in payload and isinstance(payload["args"], list):
            args = payload["args"]
        else:
            src = payload.get("source")
            if not src or not str(src).startswith("gs://"):
                return jsonify({"error": 'missing or invalid "source" (gs://...)'}), 400
            args = ["--source", str(src)]

        op = jobs.run_job(
            name=job_res(PROJECT_ID, REGION, JOB_NAME),
            overrides={"container_overrides": [{"args": args}]}
        )
        exec_name = getattr(getattr(op, "metadata", None), "name", None) or op.operation.name
        return jsonify({"status": "accepted", "job": JOB_NAME, "region": REGION, "execution": exec_name}), 202
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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
                return jsonify({"error":"missing or invalid \"source\" (gs://...)"}), 400
            args = ["--source", str(src)]
        op = jobs.run_job(
            name=job_res(PROJECT_ID, REGION, JOB_NAME),
            overrides={"container_overrides":[{"args": args}]}
        )
        exec_name = getattr(getattr(op, "metadata", None), "name", None) or op.operation.name
        return jsonify({"status":"accepted","job":JOB_NAME,"execution":exec_name}), 202
    except Exception as e:
        return jsonify({"error": str(e)}), 500
