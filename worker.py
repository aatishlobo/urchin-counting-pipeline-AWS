#!/usr/bin/env python3
import os
import sys
import csv
import json
import time
import random
import logging
import subprocess
from pathlib import Path
from datetime import datetime
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError


# =========================
# Logging
# =========================
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("sqs-worker")


# =========================
# Config
# =========================
def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


AWS_REGION = os.environ.get("AWS_REGION", "us-west-1")
SQS_QUEUE_URL = os.environ.get(
    "SQS_QUEUE_URL",
    "https://sqs.us-west-1.amazonaws.com/129999530657/firstTrySNS",
)

# SQS polling
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", "10"))
WAIT_TIME_SECONDS = int(os.environ.get("WAIT_TIME_SECONDS", "20"))
SLEEP_WHEN_EMPTY = float(os.environ.get("SLEEP_WHEN_EMPTY", "2.0"))
EMPTY_GRACE_PERIOD_SECONDS = int(os.environ.get("EMPTY_GRACE_PERIOD_SECONDS", "60"))

# Download behavior
DOWNLOAD_DIR = Path(os.environ.get("DOWNLOAD_DIR", "/tmp/sqs_downloads"))
DELETE_MESSAGES = _env_bool("DELETE_MESSAGES", True)

# S3 existence retries
S3_EXIST_MAX_ATTEMPTS = int(os.environ.get("S3_EXIST_MAX_ATTEMPTS", "5"))
S3_EXIST_BASE_SLEEP = float(os.environ.get("S3_EXIST_BASE_SLEEP", "1.0"))
S3_EXIST_MAX_SLEEP = float(os.environ.get("S3_EXIST_MAX_SLEEP", "8.0"))

# Batch runner
SIMPLE_RUN_SCRIPT = Path(
    os.environ.get("SIMPLE_RUN_SCRIPT", "/home/ubuntu/scripts/simple_run_transects.py")
)
BATCH_OUTPUT_DIR = Path(
    os.environ.get("BATCH_OUTPUT_DIR", "/home/ubuntu/outputs/batch_simple")
)
BATCH_S3_PREFIX = os.environ.get("BATCH_S3_PREFIX", "batch_simple/")
UPLOAD_BATCH_OUTPUTS_TO_S3 = _env_bool("UPLOAD_BATCH_OUTPUTS_TO_S3", True)

DET_SCRIPT = Path(os.environ.get("DET_SCRIPT", "/home/ubuntu/scripts/detection_gpu2_aws.py"))
PATH_SCRIPT = Path(os.environ.get("PATH_SCRIPT", "/home/ubuntu/scripts/pathway_gpu_aws.py"))
MODEL_PATH = Path(os.environ.get("MODEL_PATH", "/home/ubuntu/models/UrchinfRCNN_TNC2.pt"))
BEST_CKPT = Path(os.environ.get("BEST_CKPT", "/home/ubuntu/models/UrchinfRCNN_TNC2_best.pt"))
BATCH_PYTHON_EXEC = os.environ.get("BATCH_PYTHON_EXEC", sys.executable)
SUMMARY_NAME = os.environ.get("SUMMARY_NAME", "summary.jsonl")

# S3 outputs
OUTPUT_S3_BUCKET = os.environ.get("OUTPUT_S3_BUCKET", "final-script-outputs")
MASTER_JSONL_LOCAL = Path(os.environ.get("MASTER_JSONL_LOCAL", "/home/ubuntu/outputs/master.jsonl"))
MASTER_CSV_LOCAL = Path(os.environ.get("MASTER_CSV_LOCAL", "/home/ubuntu/outputs/master.csv"))
MASTER_CSV_S3_KEY = os.environ.get("MASTER_CSV_S3_KEY", "csvs/master.csv")

# Stopper coordination
PROCESSING_STATE_BUCKET = os.environ.get("PROCESSING_STATE_BUCKET", OUTPUT_S3_BUCKET)
PROCESSING_STATE_KEY = os.environ.get("PROCESSING_STATE_KEY", "autostopper/processing_state.json")
WORKER_ID = os.environ.get("WORKER_ID", "ec2-worker-1")

# Annotation
ANNOTATION_SCRIPT = Path(
    os.environ.get("ANNOTATION_SCRIPT", "/home/ubuntu/scripts/video_result.py")
)
ANNOTATION_OUTPUT_DIR = Path(
    os.environ.get("ANNOTATION_OUTPUT_DIR", "/home/ubuntu/outputs/annotated_videos")
)
ANNOTATION_S3_PREFIX = os.environ.get("ANNOTATION_S3_PREFIX", "annotated_videos/")
UPLOAD_ANNOTATED_VIDEO_TO_S3 = _env_bool("UPLOAD_ANNOTATED_VIDEO_TO_S3", True)
DELETE_LOCAL_ANNOTATED_VIDEO_AFTER_UPLOAD = _env_bool(
    "DELETE_LOCAL_ANNOTATED_VIDEO_AFTER_UPLOAD", False
)


# =========================
# AWS clients
# =========================
sqs = boto3.client("sqs", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


# =========================
# Generic helpers
# =========================
def _prefix(p: str) -> str:
    p = (p or "").strip()
    if p and not p.endswith("/"):
        p += "/"
    return p


def upload_file_to_s3(local_path: Path, bucket: str, key: str) -> None:
    log.info("Uploading %s -> s3://%s/%s", local_path, bucket, key)
    s3.upload_file(str(local_path), bucket, key)


def upload_directory_to_s3(local_dir: Path, bucket: str, s3_prefix: str) -> None:
    pref = _prefix(s3_prefix)
    for p in local_dir.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(local_dir).as_posix()
        key = f"{pref}{rel}"
        upload_file_to_s3(p, bucket, key)


# =========================
# Stopper helpers
# =========================
def set_processing_state(active: bool, extra: dict | None = None) -> None:
    payload = {
        "active": active,
        "worker_id": WORKER_ID,
        "timestamp": int(time.time()),
    }
    if extra:
        payload.update(extra)

    s3.put_object(
        Bucket=PROCESSING_STATE_BUCKET,
        Key=PROCESSING_STATE_KEY,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )


def clear_processing_state() -> None:
    set_processing_state(False)


# =========================
# SQS / S3 ingestion helpers
# =========================
def safe_local_path(bucket: str, key: str) -> Path:
    dest = DOWNLOAD_DIR / bucket / Path(key)
    dest.parent.mkdir(parents=True, exist_ok=True)
    return dest


def extract_s3_records_from_body(body: str) -> list[tuple[str, str]]:
    if not body:
        return []

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return []

    if isinstance(payload, dict) and "Message" in payload and isinstance(payload["Message"], str):
        try:
            payload = json.loads(payload["Message"])
        except json.JSONDecodeError:
            return []

    if isinstance(payload, dict) and payload.get("Event") == "s3:TestEvent":
        return []

    records = payload.get("Records") if isinstance(payload, dict) else None
    if not records or not isinstance(records, list):
        return []

    out: list[tuple[str, str]] = []
    for r in records:
        try:
            bucket = r["s3"]["bucket"]["name"]
            key = unquote_plus(r["s3"]["object"]["key"])
            out.append((bucket, key))
        except Exception:
            continue
    return out


def s3_object_exists_with_retry(bucket: str, key: str) -> bool:
    for attempt in range(1, S3_EXIST_MAX_ATTEMPTS + 1):
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            missing = (status == 404) or (code in ("404", "NoSuchKey", "NotFound"))

            if missing:
                if attempt == S3_EXIST_MAX_ATTEMPTS:
                    return False
                sleep = min(
                    S3_EXIST_MAX_SLEEP,
                    S3_EXIST_BASE_SLEEP * (2 ** (attempt - 1)),
                ) + random.uniform(0, 0.25)
                log.warning(
                    "HeadObject 404 for s3://%s/%s (attempt %d/%d); retrying in %.2fs",
                    bucket, key, attempt, S3_EXIST_MAX_ATTEMPTS, sleep,
                )
                time.sleep(sleep)
                continue

            raise


def download_object(bucket: str, key: str) -> Path:
    dest = safe_local_path(bucket, key)
    log.info("Downloading s3://%s/%s -> %s", bucket, key, dest)
    s3.download_file(bucket, key, str(dest))
    return dest


def queue_counts() -> tuple[int, int]:
    attrs = sqs.get_queue_attributes(
        QueueUrl=SQS_QUEUE_URL,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    visible = int(attrs.get("ApproximateNumberOfMessages", "0"))
    inflight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", "0"))
    return visible, inflight


def process_message_collect_paths(msg: dict, printed_so_far: int) -> tuple[int, list[Path]]:
    receipt_handle = msg["ReceiptHandle"]
    body = msg.get("Body", "")

    records = extract_s3_records_from_body(body)
    if not records:
        log.warning("Message did not contain S3 Records[]; skipping.")
        if DELETE_MESSAGES:
            try:
                sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
                log.info("Deleted non-S3 message (success).")
            except ClientError as e:
                log.error("Failed to delete non-S3 message: %s", e)
        return printed_so_far, []

    counter = printed_so_far
    downloaded: list[Path] = []

    for bucket, key in records:
        counter += 1
        print(f"{counter:06d}  bucket={bucket}  key={key}", flush=True)

        exists = s3_object_exists_with_retry(bucket, key)
        if not exists:
            log.error("Object missing after retries; deleting SQS message: s3://%s/%s", bucket, key)
            if DELETE_MESSAGES:
                try:
                    sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
                    log.info("Deleted message for missing object (success).")
                except ClientError as e:
                    log.error("Failed to delete message for missing object: %s", e)
            return counter, downloaded

        downloaded.append(download_object(bucket, key))

    log.info("Downloaded %d file(s) for this message.", len(downloaded))

    if DELETE_MESSAGES:
        try:
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            log.info("Deleted message (success).")
        except ClientError as e:
            log.error("Failed to delete message: %s", e)

    return counter, downloaded


# =========================
# Batch / CSV helpers
# =========================
def append_jsonl_to_master(run_jsonl: Path, master_jsonl: Path) -> None:
    if not run_jsonl.exists():
        raise FileNotFoundError(f"Run JSONL not found: {run_jsonl}")

    master_jsonl.parent.mkdir(parents=True, exist_ok=True)

    with open(run_jsonl, "r", encoding="utf-8") as src, open(master_jsonl, "a", encoding="utf-8") as dst:
        for line in src:
            line = line.rstrip("\n")
            if line:
                dst.write(line + "\n")


def jsonl_to_csv(jsonl_path: Path, csv_path: Path) -> None:
    rows = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))

    if not rows:
        raise ValueError(f"No rows found in {jsonl_path}")

    fieldnames = [
        "video_filename",
        "video_path",
        "urchin_count",
        "duration_sec",
        "fps",
        "frame_count",
        "date_processed",
        "model_version",
        "detection_json",
        "pathway_json",
        "params",
    ]

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "video_filename": r.get("video_filename"),
                "video_path": r.get("video_path"),
                "urchin_count": r.get("urchin_count"),
                "duration_sec": r.get("duration_sec"),
                "fps": r.get("fps"),
                "frame_count": r.get("frame_count"),
                "date_processed": r.get("date_processed"),
                "model_version": r.get("model_version"),
                "detection_json": r.get("detection_json"),
                "pathway_json": r.get("pathway_json"),
                "params": json.dumps(r.get("params"), ensure_ascii=False),
            })


# =========================
# Annotation helpers
# =========================
def build_annotation_command(video_path: Path, json_path: Path, output_video: Path) -> list[str]:
    return [
        sys.executable,
        str(ANNOTATION_SCRIPT),
        "--video_path", str(video_path),
        "--json_path", str(json_path),
        "--output_video", str(output_video),
    ]


def run_annotation(video_path: Path, pathway_json: Path, output_dir: Path) -> Path | None:
    if not ANNOTATION_SCRIPT.exists():
        raise FileNotFoundError(f"Annotation script not found: {ANNOTATION_SCRIPT}")
    if not video_path.exists():
        log.error("Video missing for annotation: %s", video_path)
        return None
    if not pathway_json.exists():
        log.error("Pathway JSON missing for annotation: %s", pathway_json)
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    out_video = output_dir / f"{video_path.stem}.annotated.mp4"

    cmd = build_annotation_command(video_path, pathway_json, out_video)
    log.info("Running annotation: %s", " ".join(cmd))
    res = subprocess.run(cmd, text=True, capture_output=True)

    if res.returncode != 0:
        log.error("Annotation failed for %s", video_path)
        if res.stdout:
            log.error("ANNOTATION STDOUT:\n%s", res.stdout)
        if res.stderr:
            log.error("ANNOTATION STDERR:\n%s", res.stderr)
        return None

    if not out_video.exists():
        log.error("Annotation completed but output missing: %s", out_video)
        return None

    return out_video


# =========================
# Batch runner
# =========================
def run_simple_batch_on_videos(downloaded_files: list[Path]) -> None:
    """
    For this drain cycle:
      - run simple_run_transects.py on each downloaded file
      - convert summary.jsonl -> summary.csv
      - update master.jsonl/master.csv
      - upload CSVs
      - generate and upload annotated videos
      - upload run directory outputs
    """
    if not downloaded_files:
        log.info("No downloaded files in this drain cycle; skipping batch run.")
        return

    set_processing_state(True, {"stage": "batch_start", "file_count": len(downloaded_files)})

    try:
        if not SIMPLE_RUN_SCRIPT.exists():
            raise FileNotFoundError(f"simple_run_transects.py not found: {SIMPLE_RUN_SCRIPT}")
        if not DET_SCRIPT.exists():
            raise FileNotFoundError(f"Detection script not found: {DET_SCRIPT}")
        if not PATH_SCRIPT.exists():
            raise FileNotFoundError(f"Pathway script not found: {PATH_SCRIPT}")
        if not MODEL_PATH.exists():
            raise FileNotFoundError(f"Model file not found: {MODEL_PATH}")
        if not BEST_CKPT.exists():
            raise FileNotFoundError(f"Best checkpoint not found: {BEST_CKPT}")
        if not ANNOTATION_SCRIPT.exists():
            raise FileNotFoundError(f"Annotation script not found: {ANNOTATION_SCRIPT}")

        run_tag = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        run_out_dir = BATCH_OUTPUT_DIR / f"run_{run_tag}"
        run_out_dir.mkdir(parents=True, exist_ok=True)
        summary_jsonl = run_out_dir / SUMMARY_NAME

        log.info(
            "Batch run (simple_run_transects.py) on %d file(s). Out dir: %s",
            len(downloaded_files),
            run_out_dir,
        )

        for video_path in downloaded_files:
            if not video_path.exists():
                log.warning("Downloaded file missing; skipping: %s", video_path)
                continue

            set_processing_state(
                True,
                {"stage": "running_simple_run", "video": video_path.name, "run_dir": str(run_out_dir)},
            )

            cmd = [
                BATCH_PYTHON_EXEC,
                "-u",
                str(SIMPLE_RUN_SCRIPT),
                "--input", str(video_path),
                "--out_dir", str(run_out_dir),
                "--python", str(BATCH_PYTHON_EXEC),
                "--det_script", str(DET_SCRIPT),
                "--path_script", str(PATH_SCRIPT),
                "--model_path", str(MODEL_PATH),
                "--best_ckpt", str(BEST_CKPT),
                "--summary_name", SUMMARY_NAME,
            ]

            log.info("Running batch cmd: %s", " ".join(cmd))
            res = subprocess.run(cmd, text=True, capture_output=True)
            if res.returncode != 0:
                log.error("Batch script failed for %s", video_path)
                if res.stdout:
                    log.error("BATCH STDOUT:\n%s", res.stdout)
                if res.stderr:
                    log.error("BATCH STDERR:\n%s", res.stderr)
                continue

            if res.stdout:
                log.info("Batch stdout:\n%s", res.stdout.strip())

        if summary_jsonl.exists():
            summary_csv = run_out_dir / "summary.csv"
            try:
                set_processing_state(True, {"stage": "building_run_csv", "run_dir": str(run_out_dir)})
                jsonl_to_csv(summary_jsonl, summary_csv)
                log.info("Wrote CSV summary: %s", summary_csv)

                set_processing_state(True, {"stage": "updating_master_jsonl"})
                append_jsonl_to_master(summary_jsonl, MASTER_JSONL_LOCAL)
                log.info("Updated local master JSONL: %s", MASTER_JSONL_LOCAL)

                set_processing_state(True, {"stage": "rebuilding_master_csv"})
                jsonl_to_csv(MASTER_JSONL_LOCAL, MASTER_CSV_LOCAL)
                log.info("Rebuilt local master CSV: %s", MASTER_CSV_LOCAL)

                if UPLOAD_BATCH_OUTPUTS_TO_S3:
                    set_processing_state(True, {"stage": "uploading_master_csv"})
                    upload_file_to_s3(MASTER_CSV_LOCAL, OUTPUT_S3_BUCKET, MASTER_CSV_S3_KEY)
                    log.info(
                        "Uploaded updated master CSV to s3://%s/%s",
                        OUTPUT_S3_BUCKET,
                        MASTER_CSV_S3_KEY,
                    )

                    set_processing_state(True, {"stage": "uploading_run_csv"})
                    csvs_key = f"csvs/{run_out_dir.name}.csv"
                    upload_file_to_s3(summary_csv, OUTPUT_S3_BUCKET, csvs_key)
                    log.info("Uploaded summary CSV to s3://%s/%s", OUTPUT_S3_BUCKET, csvs_key)

            except Exception:
                log.error("Failed to convert/upload CSVs", exc_info=True)
        else:
            log.warning("No summary.jsonl found at %s (batch may have failed).", summary_jsonl)

        if UPLOAD_ANNOTATED_VIDEO_TO_S3:
            set_processing_state(True, {"stage": "rendering_annotated_videos"})
            for video_path in downloaded_files:
                try:
                    if not video_path.exists():
                        log.warning("Downloaded file missing for annotation; skipping: %s", video_path)
                        continue

                    pathway_json = run_out_dir / video_path.stem / "pathway.json"

                    out_video = run_annotation(
                        video_path=video_path,
                        pathway_json=pathway_json,
                        output_dir=ANNOTATION_OUTPUT_DIR / run_out_dir.name,
                    )

                    if out_video:
                        s3_key = f"{_prefix(ANNOTATION_S3_PREFIX)}{run_out_dir.name}/{out_video.name}"
                        upload_file_to_s3(out_video, OUTPUT_S3_BUCKET, s3_key)
                        log.info("Uploaded annotated video to s3://%s/%s", OUTPUT_S3_BUCKET, s3_key)

                        if DELETE_LOCAL_ANNOTATED_VIDEO_AFTER_UPLOAD:
                            out_video.unlink(missing_ok=True)
                            log.info("Deleted local annotated video: %s", out_video)

                except Exception:
                    log.error("Annotated video generation/upload failed", exc_info=True)

        if UPLOAD_BATCH_OUTPUTS_TO_S3:
            set_processing_state(True, {"stage": "uploading_run_directory"})
            s3_prefix = f"{_prefix(BATCH_S3_PREFIX)}{run_out_dir.name}/"
            log.info("Uploading batch outputs to s3://%s/%s", OUTPUT_S3_BUCKET, s3_prefix)
            upload_directory_to_s3(run_out_dir, OUTPUT_S3_BUCKET, s3_prefix)

    finally:
        clear_processing_state()


# =========================
# Main loop
# =========================
def main() -> None:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Starting SQS worker")
    log.info("AWS_REGION=%s", AWS_REGION)
    log.info("SQS_QUEUE_URL=%s", SQS_QUEUE_URL)
    log.info("DELETE_MESSAGES=%s", DELETE_MESSAGES)
    log.info("DOWNLOAD_DIR=%s", DOWNLOAD_DIR)
    log.info("EMPTY_GRACE_PERIOD_SECONDS=%s", EMPTY_GRACE_PERIOD_SECONDS)
    log.info("SIMPLE_RUN_SCRIPT=%s", SIMPLE_RUN_SCRIPT)
    log.info("ANNOTATION_SCRIPT=%s", ANNOTATION_SCRIPT)
    log.info("MODEL_PATH=%s", MODEL_PATH)
    log.info("BEST_CKPT=%s", BEST_CKPT)
    log.info("OUTPUT_S3_BUCKET=%s", OUTPUT_S3_BUCKET)

    printed = 0
    last_message_time = time.time()
    downloaded_this_cycle: list[Path] = []

    while True:
        try:
            resp = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=max(1, min(MAX_MESSAGES, 10)),
                WaitTimeSeconds=max(0, min(WAIT_TIME_SECONDS, 20)),
                AttributeNames=["All"],
                MessageAttributeNames=["All"],
            )
        except ClientError as e:
            log.error("receive_message failed: %s", e)
            time.sleep(2)
            continue

        messages = resp.get("Messages", [])

        if messages:
            last_message_time = time.time()
            for msg in messages:
                try:
                    printed, new_files = process_message_collect_paths(msg, printed)
                    downloaded_this_cycle.extend(new_files)
                except Exception:
                    log.error("process_message failed", exc_info=True)
            continue

        idle = time.time() - last_message_time

        if idle >= EMPTY_GRACE_PERIOD_SECONDS:
            try:
                visible, inflight = queue_counts()
                log.info(
                    "Queue idle for %ds. Queue counts: visible=%d inflight=%d",
                    int(idle), visible, inflight,
                )
                if inflight == 0:
                    run_simple_batch_on_videos(downloaded_this_cycle)
                    downloaded_this_cycle.clear()
                    last_message_time = time.time()
            except Exception:
                log.error("Batch phase error", exc_info=True)
                downloaded_this_cycle.clear()
                last_message_time = time.time()

        time.sleep(SLEEP_WHEN_EMPTY)


if __name__ == "__main__":
    main()
