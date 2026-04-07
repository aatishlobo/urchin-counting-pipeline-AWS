# 🌊 Kelp Forest Monitoring Pipeline

![AWS](https://img.shields.io/badge/AWS-S3%2FSQS%2FEC2%2FLambda-orange)
![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Status](https://img.shields.io/badge/status-production-green)
![GPU](https://img.shields.io/badge/GPU-Tesla%20T4-red)

---

## 🏗️ Architecture Diagram

![Architecture Diagram](pipeline_architecture.png)

---

## 📸 Example Output

### Annotated Video Frame
![Annotated Example](https://via.placeholder.com/800x400.png?text=Annotated+Video+Frame)

### Data Output (CSV)
![CSV Example](https://via.placeholder.com/800x300.png?text=CSV+Output+Preview)

---

## Overview

This project implements a fully automated, event-driven data pipeline for processing underwater video data.

---

## Key Features

- Event-driven ingestion (S3)
- Distributed processing (SQS + EC2 GPU)
- ML pipeline (detection → tracking → annotation)
- Automated scaling (Lambda start/stop)
- Structured outputs (JSONL → CSV)

---

## Architecture

```
Upload → S3 → SQS → EC2 Worker → ML Pipeline → S3 Outputs
                ↑
          Lambda (start/stop)
```

---

## Outputs

- Annotated videos
- Detection + tracking JSON
- Per-run CSV
- Master CSV

---

## Run

```
sudo systemctl restart sqs-worker.service
sudo journalctl -u sqs-worker.service -f
```

---

## Notes

- Place `pipeline_architecture.png` in the root of your repo
- Replace placeholder images with real outputs for best presentation
