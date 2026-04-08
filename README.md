# Kelp Forest Monitoring Pipeline (AWS-Based Video Processing System)

## Overview

This project implements a fully automated, event-driven data pipeline for processing underwater video data collected at marine dive sites. The system ingests large `.mp4` video files, runs machine learning inference to detect and track sea urchins, generates annotated videos, and outputs structured datasets for downstream analysis.

The pipeline is built on Amazon Web Services (AWS) and is designed to handle GB-scale video ingestion, asynchronous processing, and cost-efficient compute orchestration.

---

## Key Features

- Event-driven ingestion from S3 uploads  
- Distributed processing using SQS + EC2 (GPU-enabled)  
- Multi-stage ML pipeline (detection → tracking → annotation → aggregation)  
- Automated instance lifecycle management (start + auto-stop)  
- Scalable handling of large video datasets (multipart uploads supported)  
- Structured outputs (JSONL → CSV) and annotated video artifacts  

---

## Architecture

```
[ Upload Client ]
        ↓
    [ S3 Bucket ]
        ↓
   ┌───────────────┬────────────────┐
   ↓                                ↓
[ EventBridge ]              [ S3 → SQS Notification ]
   ↓                                ↓
[ Starter Lambda ]              [ SQS Queue ]
   ↓                                ↓
[ EC2 Worker (GPU) ] ←──────────────┘
        ↓
 Download → Process → Aggregate
        ↓
   [ S3 Outputs ]
        ↓
[ Stopper Lambda ]
```

---

## End-to-End Flow

### 1. Ingestion
- Upload `.mp4` video to S3
- Triggers:
  - EventBridge → starts EC2
  - SQS message → queues job

### 2. Worker Processing
- EC2 polls SQS
- Downloads files to `/tmp/sqs_downloads/`

### 3. ML Pipeline
- Detection (PyTorch)
- Tracking (pathways)
- Batch aggregation (`summary.jsonl`)
- Annotation (`.mp4`)
- Conversion to CSV

### 4. Outputs
Stored in S3:
```
batch_simple/run_<timestamp>/
annotated_videos/run_<timestamp>/
csvs/master.csv
```

### 5. Auto Stop
- Lambda checks:
  - Queue empty
  - No processing active
- Stops EC2

---

## Components

- S3 – storage
- SQS – queue
- EC2 – GPU processing
- Lambda – orchestration
- EventBridge – scheduling

---

## Outputs

- Annotated videos
- Detection + tracking JSON
- Per-run CSV
- Master CSV

---
## Worker Service (`worker.py`)

The EC2 instance runs a Python worker under `systemd`. This script is the operational core of the pipeline. It is responsible for draining SQS, downloading S3 objects, running the batch processor, building CSV artifacts, generating annotated videos, uploading outputs, and signaling processing state so the stopper Lambda does not terminate the instance too early.

### What `worker.py` does

1. Polls the SQS queue using long polling.
2. Extracts S3 bucket/key pairs from each message.
3. Confirms the object exists in S3 with retry logic.
4. Downloads videos locally to a working directory.
5. After the queue is idle for a grace period, launches `simple_run_transects.py` on all downloaded files from that cycle.
6. Converts the per-run `summary.jsonl` into `summary.csv`.
7. Appends the new run into a persistent master dataset and regenerates `master.csv`.
8. Runs `video_result.py` to generate annotated videos from the original video and pathway JSON.
9. Uploads batch outputs, CSVs, and annotated videos to the configured S3 output bucket.
10. Writes processing-state markers so the stopper Lambda knows when the instance is still busy.

### Key sections in `worker.py`

#### Configuration
Environment variables control queue location, S3 output bucket, model paths, script paths, download directories, and batch output behavior.

#### SQS/S3 ingestion
The worker receives SQS messages, parses S3 event payloads, retries missing objects, downloads valid files locally, and deletes queue messages only after safe handling.

#### Batch processing
Once the queue remains idle for the configured grace period, the worker launches the combined batch script on the downloaded files for that cycle.

#### CSV generation
The worker converts per-run `summary.jsonl` into `summary.csv`, updates a persistent `master.jsonl`, and regenerates `master.csv`.

#### Annotation
For each processed video, the worker locates the generated `pathway.json` and calls `video_result.py` to produce an annotated video artifact.

#### Processing state
The worker writes a processing-state JSON object to S3 so the stopper Lambda can distinguish true idleness from active computation.

---

## Commands

```
sudo systemctl restart sqs-worker.service
sudo journalctl -u sqs-worker.service -f
```

---

## Notes

- Place `pipeline_architecture.png` in the root of your repo
- Keep `README.md` in the repo root for GitHub rendering
- Store script paths and bucket names in environment variables where possible
- Ensure IAM policies allow S3 read/write, SQS receive/delete/get attributes, and processing-state object access

---

## Summary

This pipeline transforms raw underwater video into structured ecological data through a cost-aware, event-driven AWS architecture. The worker script serves as the execution backbone, coordinating message consumption, model execution, aggregation, annotation, and artifact publication in a way that is robust enough for recurring field data workflows.
