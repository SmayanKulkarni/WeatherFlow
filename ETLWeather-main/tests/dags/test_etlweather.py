"""Tests for weather_etl_pipeline: structure, transform, validation, and load."""

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowFailException
from airflow.models import DagBag
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_RESPONSE = {
    "current_weather": {
        "time": "2026-10-25T10:30",
        "temperature": 12.7,
        "windspeed": 27.7,
        "winddirection": 269,
        "weathercode": 1,
    }
}
PARAMS = {"latitude": 51.5074, "longitude": -0.1278}


@pytest.fixture(scope="module")
def dag():
    dag_bag = DagBag(include_examples=False)
    assert "weather_etl_pipeline" in dag_bag.dags, dag_bag.import_errors
    return dag_bag.dags["weather_etl_pipeline"]


def fn(dag, task_id):
    return dag.get_task(task_id).python_callable


def test_dag_has_expected_tasks(dag):
    assert set(dag.task_ids) == {
        "create_table",
        "extract_weather_data",
        "transform_weather_data",
        "validate_weather_data",
        "load_weather_data",
    }


def test_task_dependencies(dag):
    assert dag.get_task("extract_weather_data").downstream_task_ids == {"transform_weather_data"}
    assert dag.get_task("transform_weather_data").downstream_task_ids == {"validate_weather_data"}
    assert dag.get_task("validate_weather_data").downstream_task_ids == {"load_weather_data"}
    assert dag.get_task("create_table").downstream_task_ids == {"load_weather_data"}


def test_params_default_to_london(dag):
    assert dag.params["latitude"] == 51.5074
    assert dag.params["longitude"] == -0.1278


def test_params_reject_out_of_range_latitude(dag):
    with pytest.raises(Exception):
        dag.params.update({"latitude": 123})


def test_extract_passes_params_as_query_string(dag):
    response = MagicMock(status_code=200)
    response.json.return_value = API_RESPONSE
    with patch.object(HttpHook, "run", return_value=response) as run:
        result = fn(dag, "extract_weather_data")(params=PARAMS)

    assert result == API_RESPONSE
    assert run.call_args.args[0] == "/v1/forecast"
    assert run.call_args.kwargs["data"] == {
        "latitude": 51.5074,
        "longitude": -0.1278,
        "current_weather": "true",
    }


def test_extract_raises_on_non_200(dag):
    with patch.object(HttpHook, "run", return_value=MagicMock(status_code=500)):
        with pytest.raises(Exception, match="500"):
            fn(dag, "extract_weather_data")(params=PARAMS)


def test_transform_builds_row_with_observed_at(dag):
    result = fn(dag, "transform_weather_data")(API_RESPONSE, params=PARAMS)

    assert result == {
        "latitude": 51.5074,
        "longitude": -0.1278,
        "temperature": 12.7,
        "windspeed": 27.7,
        "winddirection": 269,
        "weathercode": 1,
        "observed_at": "2026-10-25T10:30",
    }


def test_validate_passes_plausible_temperature(dag):
    row = {"temperature": 12.7}
    assert fn(dag, "validate_weather_data")(row) == row


@pytest.mark.parametrize("temperature", [None, -91, 61])
def test_validate_fails_without_retry_on_bad_temperature(dag, temperature):
    with pytest.raises(AirflowFailException):
        fn(dag, "validate_weather_data")({"temperature": temperature})


def test_load_inserts_with_conflict_guard(dag):
    row = fn(dag, "transform_weather_data")(API_RESPONSE, params=PARAMS)
    with patch.object(PostgresHook, "run") as run:
        fn(dag, "load_weather_data")(row)

    sql = run.call_args.args[0]
    assert "ON CONFLICT (latitude, longitude, observed_at) DO NOTHING" in sql
    assert run.call_args.kwargs["parameters"] == (
        51.5074, -0.1278, 12.7, 27.7, 269, 1, "2026-10-25T10:30"
    )
