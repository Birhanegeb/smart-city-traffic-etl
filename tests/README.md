# Testing and Benchmarking Record

This document records the testing, integration validation, performance benchmarking, and scalability evaluation performed for the Smart City Traffic ETL Pipeline.

The evaluation provides empirical evidence for the pipeline's reliability, failure isolation, reproducibility, multi-city processing, and baseline scalability.

---

## 1. Test Environment

The complete pipeline environment was started using Docker Compose:

```bash
docker compose up -d
```

The following services started successfully:

* Apache Spark
* PostgreSQL
* Apache Airflow
* Apache Superset

The tests were executed inside the Airflow container to ensure that the testing environment was consistent with the project deployment environment.

---

## 2. Automated Test Execution

The complete automated test suite was executed with:

```bash
docker compose exec airflow-webserver pytest -q /opt/airflow/tests
```

The project uses `pytest` and includes tests covering:

* Unit and component-level validation
* Configuration validation
* Integration testing
* Failure-isolation testing
* Grid-generation benchmarking
* Spark data-volume benchmarking
* Latest-batch micro-batch behavior

---

## 3. Failure-Isolation Test

A deterministic failure-isolation test was implemented in:

```text
tests/test_ingestion_failure_isolation.py
```

The test was executed with:

```bash
docker compose exec airflow-webserver \
pytest -q -s /opt/airflow/tests/test_ingestion_failure_isolation.py
```

### Result

```text
[berlin] failed (52.47,13.29): temporary API failure
[berlin] success=2, failed=1, time=0.001s

1 passed in 0.57s
```

### Interpretation

The test demonstrates that an individual API request failure does not abort the entire city ingestion process.

Successful requests continue to be processed, while the failed request is recorded and counted as dropped.

This provides empirical evidence for the pipeline's **failure-isolation behavior**, which is an important component of its resilience design.

---

## 4. Medallion Architecture Integration Test

The integration test is implemented in:

```text
tests/test_spark_pipeline_integration.py
```

The test validates the complete transformation flow:

```text
Raw JSONL
    ↓
Bronze Parquet
    ↓
Silver Parquet
    ↓
Gold Parquet
```

The test verifies:

* Invalid-record filtering
* Duplicate removal
* Derived-field generation
* Congestion aggregation
* Multi-city Gold output
* Successful execution of the Bronze → Silver → Gold workflow

Temporary local Spark directories are used so that the test can execute without modifying the project's production data directories.

### Result

```text
1 passed in 6.04s
```

### Interpretation

The successful integration test demonstrates that the main medallion processing stages can execute together and produce the expected layered outputs.

---

## 5. Grid-Generation Benchmark

Grid generation was benchmarked using three grid dimensions.

The benchmark was executed with:

```bash
docker compose exec airflow-webserver \
pytest -q -s /opt/airflow/tests/test_benchmark_smoke.py
```

### Results

| Grid Dimension | Points | Execution Time (s) | Throughput (points/s) |
| -------------: | -----: | -----------------: | --------------------: |
|          5 × 5 |     25 |           0.000025 |            982,781.69 |
|        10 × 10 |    100 |           0.000067 |          1,494,500.24 |
|        20 × 20 |    400 |           0.000274 |          1,459,624.95 |

### Interpretation

Grid generation completed successfully for all tested configurations.

The number of generated points increased from 25 to 100 and then to 400 as the grid dimension increased from 5 to 10 and 20.

The measured throughput remained approximately between 0.98 and 1.49 million points per second.

These results demonstrate that the grid-generation component introduces very little computational overhead for the tested configurations and provides a reproducible baseline for multi-city grid construction.

---

## 6. Spark Data-Volume Benchmark

Spark processing was evaluated using datasets containing 100, 1,000, and 5,000 records.

The benchmark was executed with:

```bash
docker compose exec airflow-webserver \
pytest -q -s /opt/airflow/tests/test_spark_scalability.py
```

### Results

| Records | Execution Time (s) | Records/s |
| ------: | -----------------: | --------: |
|     100 |           0.129059 |    774.84 |
|   1,000 |           0.121626 |  8,221.91 |
|   5,000 |           0.120086 | 41,636.76 |

### Interpretation

All three dataset sizes were processed successfully.

Execution time remained approximately constant at around 0.12 seconds, while throughput increased substantially as the input size increased.

This behavior is consistent with fixed Spark session and execution overhead dominating these relatively small experiments. As the input size increases, the fixed overhead is amortized over a larger number of records, resulting in higher measured throughput.

The results therefore provide a **reproducible baseline scalability measurement** for the Spark processing component.

These experiments should not be interpreted as proof of production-scale scalability because the tested datasets are relatively small and the experiments were conducted in the available local/containerized environment.

---

## 7. Complete Automated Test Suite

The complete test suite was executed using:

```bash
docker compose exec airflow-webserver \
pytest -q -s /opt/airflow/tests
```

### Result

```text
RemovedInAirflow3Warning: Param `schedule_interval` is deprecated and will be removed in a future release. Please use `schedule` instead.

grid_steps=5, points=25, elapsed_seconds=0.000025, points_per_second=982781.69
grid_steps=10, points=100, elapsed_seconds=0.000067, points_per_second=1494500.24
grid_steps=20, points=400, elapsed_seconds=0.000274, points_per_second=1459624.95

berlin: 0.000021 seconds, 25 points
bremen: 0.000013 seconds, 16 points
frankfurt: 0.000018 seconds, 25 points

Total: 0.000063 seconds, 66 points

[berlin] failed (52.47,13.29): temporary API failure
[berlin] success=2, failed=1, time=0.001s

records=100, elapsed_seconds=0.129059, records_per_second=774.84
records=1000, elapsed_seconds=0.121626, records_per_second=8221.91
records=5000, elapsed_seconds=0.120086, records_per_second=41636.76

10 passed in 7.22s
```

The Airflow `schedule_interval` message is a deprecation warning and does not indicate a test failure.

### Test Result Summary

**10 tests passed successfully.**

The complete test run validates:

* Multi-city grid generation
* Configuration-driven city processing
* API failure isolation
* Medallion pipeline integration
* Spark data-volume benchmarking
* Latest-batch micro-batch behavior
* General pipeline component correctness

The benchmark results show increasing throughput as the dataset size increases. However, the experiments represent a controlled baseline rather than a production-scale performance evaluation.

---

## 8. Manual Spark Pipeline Execution

In addition to the automated tests, the actual Spark pipeline stages were executed against project data.

The micro-batch processing flow is:

```text
TomTom Traffic API
        ↓
    Raw JSONL
        ↓
Bronze: select latest batch
        ↓
Silver: select and deduplicate latest batch
        ↓
Gold: process latest hour
        ↓
PostgreSQL / Superset
```

Gold processing is triggered after four completed Spark cycles during normal pipeline operation.

### 8.1 Directory Permissions

Before executing the Spark jobs, the mounted output directories were made writable by the Spark container user:

```bash
chmod -R a+rwX data/bronze/tomtom_segments

mkdir -p data/silver/tomtom_segments
chmod -R a+rwX data/silver/tomtom_segments

mkdir -p data/gold/traffic_dashboard
chmod -R a+rwX data/gold/traffic_dashboard
```

During testing, stale Spark commit markers were removed when they prevented a new append:

```bash
rm -f data/bronze/tomtom_segments/_SUCCESS \
      data/bronze/tomtom_segments/._SUCCESS.crc

rm -f data/silver/tomtom_segments/_SUCCESS \
      data/silver/tomtom_segments/._SUCCESS.crc
```

---

### 8.2 Bronze Stage

The Bronze stage was executed with:

```bash
docker compose exec spark-master \
/opt/spark/bin/spark-submit --master local[2] \
/opt/spark-apps/bronze_job.py
```

### Result

```text
stage=bronze records_read=66 records_written=66 records_dropped=0 elapsed_seconds=5.55
```

The Bronze stage successfully processed the current micro-batch.

Bronze uses idempotent dynamic partition overwrite to prevent unintended duplication when processing repeated micro-batches.

---

### 8.3 Silver Stage

The Silver stage was executed with:

```bash
docker compose exec spark-master \
/opt/spark/bin/spark-submit --master local[2] \
/opt/spark-apps/silver_job.py
```

### Result

```text
stage=silver records_read=66 records_written=66 records_dropped=0 elapsed_seconds=5.87
```

The Silver stage processed the current Bronze micro-batch successfully.

Silver selects the maximum `batch_ts` before processing, ensuring that the current micro-batch is processed rather than repeatedly transforming older Bronze data.

---

### 8.4 Gold Stage

The Gold stage requires the PostgreSQL JDBC driver and was executed with:

```bash
docker compose exec spark-master \
/opt/spark/bin/spark-submit --master local[2] \
--jars /opt/spark/drivers/postgresql-42.7.11.jar \
/opt/spark-apps/gold_job.py
```

### Result

```text
stage=gold records_read=7854 records_written=66 records_dropped=7788 elapsed_seconds=7.92
```

The Gold stage reads accumulated Silver partitions and filters the data to the latest hour. Therefore, its input count is not expected to match the current Silver micro-batch count.

The difference between `records_read` and `records_written` is therefore expected for this processing design.

Gold output was successfully generated for:

* Berlin
* Bremen
* Frankfurt

The resulting Gold records were written to PostgreSQL for downstream dashboarding and analysis.

---

## 9. Layer-Level Validation

The observed execution results are consistent with the expected relationships between the medallion layers:

```text
Silver records_read <= Bronze records_written

Silver records_written <= Silver records_read

Gold records_written <= Gold records_read
```

The actual execution produced:

| Stage  | Records Read | Records Written | Records Dropped | Execution Time |
| ------ | -----------: | --------------: | --------------: | -------------: |
| Bronze |           66 |              66 |               0 |         5.55 s |
| Silver |           66 |              66 |               0 |         5.87 s |
| Gold   |        7,854 |              66 |           7,788 |         7.92 s |

The Bronze and Silver stages operate on the current micro-batch, while Gold reads accumulated Silver data and applies the latest-hour filter.

---

## 10. Overall Interpretation

The completed testing and benchmarking provide empirical evidence for several properties of the proposed pipeline.

### 10.1 Reliability

The automated test suite completed successfully with:

```text
10 passed in 7.22s
```

The integration tests verify the main data transformation path and important data-quality operations.

### 10.2 Failure Isolation

The deterministic API failure test demonstrates that an individual failed request does not terminate the complete city ingestion process.

Successful requests continue to be processed, while the failed request is recorded as dropped.

### 10.3 Medallion Processing

The manual execution of the Bronze, Silver, and Gold stages successfully produced the expected layered outputs.

Measured execution times were:

* Bronze: **5.55 seconds**
* Silver: **5.87 seconds**
* Gold: **7.92 seconds**

### 10.4 Multi-City Processing

The pipeline successfully generated grid configurations and Gold outputs for:

* Berlin
* Bremen
* Frankfurt

The grid benchmark also demonstrates successful processing of increasing grid sizes.

### 10.5 Reproducibility

The tests can be reproduced inside the Dockerized project environment using the documented Docker Compose and pytest commands.

The testing environment therefore provides a consistent execution context for validating the pipeline.

### 10.6 Baseline Scalability

The Spark data-volume benchmark shows increasing throughput from:

```text
774.84 records/s
```

for 100 records to:

```text
41,636.76 records/s
```

for 5,000 records.

These results demonstrate the behavior of the pipeline under the tested workloads and provide a reproducible scalability baseline.

They should not be interpreted as evidence of production-scale performance because the experiments were conducted with relatively small datasets and limited computing resources.

---

## 11. Corrections and Improvements Identified During Testing

The testing and benchmarking process also revealed issues related to repeated micro-batch processing.

The following improvements were implemented:

1. Bronze was updated to use idempotent dynamic partition overwrite.
2. Silver was updated to process only the latest Bronze `batch_ts`.
3. Automated tests were added to verify the latest-batch behavior.
4. Failure-isolation testing was added to validate handling of individual API failures.
5. Benchmark tests were added to establish reproducible performance baselines.

These changes demonstrate that testing was not only used for validation but also contributed to identifying and correcting implementation issues.

---

## 12. Limitations

The benchmarking results should be interpreted within the limitations of the experimental environment.

The Spark scalability experiments used datasets of 100, 1,000, and 5,000 records. These workloads are useful for establishing a reproducible baseline but are not sufficient to characterize production-scale performance.

Similarly, the recorded Bronze, Silver, and Gold execution times represent individual executions in the available Docker/Spark environment and should not be interpreted as statistically generalized performance estimates.

A larger-scale experiment with substantially larger datasets, repeated trials, controlled resource allocation, and multiple worker configurations would provide stronger evidence for production-scale scalability. Such an experiment was outside my available computing-resource constraints of the project.

---

## 13. Conclusion

The testing and benchmarking activities provide empirical evidence supporting the implemented pipeline architecture.

The final automated test suite achieved:

```text
10 passed
```

The evaluation demonstrated:

* Successful automated testing
* Successful Bronze → Silver → Gold integration
* Failure isolation during API errors
* Successful multi-city processing
* Correct latest-batch micro-batch handling
* Measured Bronze, Silver, and Gold execution times
* Increasing Spark throughput with larger test workloads
* Reproducible Docker-based testing

Overall, the results strengthen the empirical evaluation of the pipeline and provide measurable evidence for its reliability, resilience-related behavior, reproducibility, multi-city processing capability, and baseline scalability.

---

## 14. Acknowledgement

The testing and benchmarking recommendation provided an opportunity to evaluate the pipeline more rigorously and identify implementation issues related to repeated micro-batch processing.

As a result of this evaluation, Bronze was corrected to use idempotent writes, Silver was updated to process only the latest batch, and automated tests were introduced to verify these behaviors.

The resulting testing and benchmarking framework provides a stronger empirical foundation for the technical claims made before..
