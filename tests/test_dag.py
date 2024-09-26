# import pytest
# from airflow.models import DAG
# from airflow.models.taskinstance import TaskInstance
# from airflow.utils.dates import days_ago
# from airflow.utils.state import State

# from dags.weather_etl_dag import (
#     get_country_code,
#     get_geographical_data,
#     get_longitude_latitude,
#     process_geographical_records,
#     restructure_geographical_data,
# )

# # @pytest.fixture
# # def dag():
# #     return DAG(
# #         "test_dag",
# #         default_args={"owner": "airflow", "start_date": days_ago(1)},
# #         schedule_interval=None,
# #     )

# # def run_task(task, **kwargs):
# #     dag = kwargs.pop('dag', None)
# #     ti = TaskInstance(task=task, execution_date=dag.start_date if dag else None)
# #     context = ti.get_template_context()
# #     context.update(kwargs)
# #     return task.execute(context)

# # def test_get_country_code_single_country(dag):
# #     task = get_country_code.override(task_id="get_country_code")()
# #     result = run_task(task, dag=dag, countries="Nigeria")
# #     assert result["status"] == "success"
# #     assert result["country_codes"] == "NG"


# # def test_get_country_code_multiple_countries():
# #     result = get_country_code(["Nigeria", "Ghana"])
# #     assert result["status"] == "success"
# #     assert result["country_codes"] == ["NG", "GH"]

# # def test_get_country_code_invalid_input():
# #     result = get_country_code(123)
# #     assert result["status"] == "error"
# #     assert "Invalid input type" in result["message"]

# # @patch('dags.weather_etl_dag.get_data_from_country_code')
# # def test_get_geographical_data_single_country(mock_get_data):
# #     mock_get_data.return_value = {"weather_data": {"temp": 25, "humidity": 60}}
# #     result = get_geographical_data("NG", ["Lagos"], ["temp", "humidity"])
# #     assert result["status"] == "success"
# #     assert len(result["weather_records"]) == 1

# # @patch('dags.weather_etl_dag.get_data_from_country_code')
# # def test_get_geographical_data_multiple_countries(mock_get_data):
# #     mock_get_data.return_value = {"weather_data": {"temp": 25, "humidity": 60}}
# #     result = get_geographical_data(["NG", "GH"], ["Lagos", "Accra"], ["temp", "humidity"])
# #     assert result["status"] == "success"
# #     assert len(result["weather_records"]) == 2

# # def test_get_geographical_data_invalid_input():
# #     result = get_geographical_data("NG", "Lagos", ["temp", "humidity"])
# #     assert result["status"] == "error"
# #     assert "Invalid input type" in result["message"]

# # def test_restructure_geographical_data_valid_input():
# #     input_data = [
# #         {"name": "Lagos", "lat": 6.46, "lon": 3.39, "country": "Nigeria", "state": "Lagos"},
# #         {"name": "Accra", "lat": 5.56, "lon": -0.21, "country": "Ghana", "state": "Greater Accra"}
# #     ]
# #     result = restructure_geographical_data(input_data)
# #     assert result["status"] == "success"
# #     assert len(result["weather_fields"]["weather_fields"]) == 2
# #     assert len(result["weather_fields"]["lon_lat"]) == 2

# # def test_restructure_geographical_data_invalid_keys():
# #     input_data = [{"invalid_key": "value"}]
# #     result = restructure_geographical_data(input_data)
# #     assert result["status"] == "error"
# #     assert "Invalid keys" in result["message"]

# # def test_restructure_geographical_data_invalid_input():
# #     result = restructure_geographical_data("invalid input")
# #     assert result["status"] == "error"
# #     assert "Invalid input type" in result["message"]

# # def test_process_geographical_records_valid_input():
# #     input_data = {
# #         "weather_fields": [
# #             {"city": "Lagos", "country": "Nigeria", "state": "Lagos"},
# #             {"city": "Accra", "country": "Ghana", "state": "Greater Accra"}
# #         ],
# #         "lon_lat": [(3.39, 6.46), (-0.21, 5.56)]
# #     }
# #     result = process_geographical_records(input_data)
# #     assert len(result) == 2
# #     assert result[0]["city"] == "Lagos"
# #     assert result[1]["city"] == "Accra"

# # def test_process_geographical_records_invalid_input():
# #     result = process_geographical_records("invalid input")
# #     assert result["status"] == "error"
# #     assert "Invalid input type" in result["message"]

# # def test_get_longitude_latitude_valid_input():
# #     input_data = {
# #         "weather_fields": [
# #             {"city": "Lagos", "country": "Nigeria", "state": "Lagos"},
# #             {"city": "Accra", "country": "Ghana", "state": "Greater Accra"}
# #         ],
# #         "lon_lat": [(3.39, 6.46), (-0.21, 5.56)]
# #     }
# #     result = get_longitude_latitude(input_data)
# #     assert len(result) == 2
# #     assert result[0] == (3.39, 6.46)
# #     assert result[1] == (-0.21, 5.56)

# # def test_get_longitude_latitude_invalid_input():
# #     result = get_longitude_latitude("invalid input")
# #     assert result["status"] == "error"
# #     assert "Invalid input type" in result["message"]
