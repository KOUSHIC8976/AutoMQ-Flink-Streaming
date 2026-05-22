<div align="center">
  
  #  AutoMQ-Flink-Streaming (PermuteX Extension)
  **Decoupling Compute and Storage with AutoMQ & Apache Flink**
  
  [![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
  [![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
  [![Apache Flink](https://img.shields.io/badge/Flink-Stateful_Streaming-E6522C?style=for-the-badge&logo=apacheflink&logoColor=white)](https://flink.apache.org/)
  [![AutoMQ](https://img.shields.io/badge/AutoMQ-Cloud_Native_Kafka-000000?style=for-the-badge&logo=apachekafka&logoColor=white)](https://www.automq.com/)
  [![MinIO S3](https://img.shields.io/badge/MinIO-S3_Storage-C7202C?style=for-the-badge&logo=minio&logoColor=white)](https://min.io/)

  <p align="center">
    A proof-of-concept demonstrating a seamless migration from traditional KRaft Kafka to <b>AutoMQ (S3-backed storage)</b> with exactly <b>zero lines of code changed</b> in the Apache Flink downstream compute layer with PermuteX as foundation.
  </p>

</div>

---

##  Overview

Modern streaming architectures are moving toward the separation of compute and storage. Traditional Apache Kafka (even in KRaft mode) bundles broker compute with local disk storage, leading to expensive scaling and complex partition rebalancing.

**PermuteX Cloud-Native** proves that by leveraging **AutoMQ** (a 100% Kafka-compatible, cloud-native broker), we can push all topic data directly to AWS S3 (mocked locally via MinIO) while maintaining perfect compatibility with our existing **PyFlink** stateful processors.

---

##  Architecture & Tech Stack

1. **Ingestion Layer:** Python continuous telemetry simulator generating chaotic IoT JSON payloads.
2. **Stateless Compute Layer:** **AutoMQ**, acting purely as a compute router with no local state.
3. **Cloud Storage Layer:** **MinIO**, providing S3-compatible object storage for AutoMQ's WAL (Write-Ahead Log) and Topic Data.
4. **Stateful Processing Layer:** **Apache Flink (PyFlink)**, executing 10-second Event-Time Tumbling Windows.
5.  AutoMQ natively supports the Kafka protocol, the PyFlink SQL `CREATE TABLE` DDL required **no modifications**. Flink connects to AutoMQ exactly as it would a standard Kafka cluster, completely unaware that the underlying data is being served directly from an S3 bucket.

---

##  Key Technical Challenges Resolved

* **Cloud-Native Bootstrapping:** Bypassed traditional Zookeeper/KRaft formatting quirks by leveraging AutoMQ's consolidated `s3.data.buckets` and `s3.ops.buckets` URI configurations via Docker environment injection.
* **Event-Time Watermark Race Conditions:** Engineered the producer to guarantee continuous watermark advancement, preventing Flink memory deadlocks during late-data arrival (`WATERMARK FOR ts AS ts - INTERVAL '2' SECOND`).
* **Schema Strictness:** Ensured perfect byte-level schema alignment between the JSON producer and Flink's internal catalog to prevent silent processing halts.

---
##  Results

<img width="916" height="933" alt="image" src="https://github.com/user-attachments/assets/21fdf343-24ff-4b06-908b-c8901d21b762" />

---

<img width="1011" height="822" alt="image" src="https://github.com/user-attachments/assets/9be79ef6-143f-4b01-880e-db38440cf34e" />

---
##  Quick Start (Local S3 Simulation)

### Prerequisites
* Docker Desktop
* Python 3.10
* Java 11/17 (for Flink JVM execution)

### Execution 

**Boot the Decoupled Infrastructure (AutoMQ + MinIO)**
   ```bash
   docker-compose up -d
   python automq_producer.py
   python automq_processor.py
   ```
 **Minio Credentials : minioadmin | minioadminpassword**
---

##  Demo

https://github.com/user-attachments/assets/06fb6031-38bd-4fd3-9a83-c031d0efbe17

---

