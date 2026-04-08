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

## Performance

- ~2.5h → parallel GPU pipeline
- Scales across multiple uploads

---

## Commands

```
sudo systemctl restart sqs-worker.service
sudo journalctl -u sqs-worker.service -f
```

---

## Summary

A scalable, fault-tolerant pipeline converting raw underwater video into structured ecological data automatically.
