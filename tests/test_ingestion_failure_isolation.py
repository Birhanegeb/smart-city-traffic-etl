import json

from dags import tomtom_ingestion_api_dag as ingestion


def test_failed_point_does_not_abort_city_ingestion(monkeypatch, tmp_path):
    points = [(52.46, 13.28), (52.47, 13.29), (52.48, 13.30)]
    metrics = []

    def fake_fetch(lat, lon):
        if (lat, lon) == points[1]:
            raise RuntimeError("temporary API failure")
        return {
            "flowSegmentData": {
                "frc": "FRC1",
                "currentSpeed": 35,
                "freeFlowSpeed": 50,
                "currentTravelTime": 120,
                "confidence": 0.9,
                "roadClosure": False,
            }
        }

    monkeypatch.setattr(ingestion, "RAW_DIR", tmp_path)
    monkeypatch.setattr(ingestion, "build_grid", lambda bbox, steps: points)
    monkeypatch.setattr(ingestion, "fetch_flow_segment", fake_fetch)
    monkeypatch.setattr(
        ingestion,
        "insert_pipeline_metric",
        lambda *args: metrics.append(args),
    )

    ingestion.fetch_api_data(
        "berlin",
        ts_nodash="20260906T120000",
        dag=type("Dag", (), {"dag_id": "test"})(),
        task=type("Task", (), {"task_id": "fetch_berlin"})(),
        run_id="test-run",
    )

    output_path = tmp_path / "tomtom_berlin_20260906T120000.jsonl"
    records = [json.loads(line) for line in output_path.read_text().splitlines()]

    assert len(records) == 2
    assert {record["lat"] for record in records} == {52.46, 52.48}
    assert metrics[0][3:6] == (3, 2, 1)
    assert metrics[0][7] == "success"