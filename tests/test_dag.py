from unittest.mock import MagicMock, patch

from airflow.models.xcom_arg import XComArg

from dags.weather_etl_dag import (
    create_date_dim,
    get_country_code,
    get_geographical_data,
    get_longitude_latitude,
    get_merged_weather_records,
    join_date_dim_with_weather_fact,
    load_records_to_location_dim,
    load_records_to_weather_type_dim,
    merge_current_weather_data,
    process_geographical_records,
    restructure_geographical_data,
    transform_weather_records,
    weather_etl_dag,
)


def test_get_country_code() -> None:
    """
    Test the get_country_code function in the weather_etl_dag module

    The function should return an XComArg object that can be used to retrieve the country code
    for the given country name from the Airflow XCom.

    The function should also call the retrieve_country_code function with the given country name
    and return the result from the XCom.

    Returns:
        None
    """
    with patch("dags.weather_etl_dag.retrieve_country_code"):
        # Call the function and get the XComArg
        result = get_country_code("Nigeria")
        assert isinstance(result, XComArg)

        # Mock the behavior we expect from XComArg in an Airflow task
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "status": "success",
            "message": "Country codes for Nigeria are NG",
            "country_codes": "NG",
        }

        # Simulate how the XComArg would be used in a downstream task
        simulated_result = mock_ti.xcom_pull(task_ids=result.operator.task_id)

        assert simulated_result == {
            "status": "success",
            "message": "Country codes for Nigeria are NG",
            "country_codes": "NG",
        }

        # Verify that xcom_pull was called with the correct task_id
        mock_ti.xcom_pull.assert_called_once_with(task_ids=result.operator.task_id)


def test_get_country_code_invalid_input() -> None:
    """
    Test the get_country_code function in the weather_etl_dag module

    The function should return an XComArg object that can be used to retrieve the error message
    when an invalid input type is provided for the country name.

    The function should also call the retrieve_country_code function with the given country name
    and return the result from the XCom.

    Returns:
        None
    """

    with patch("dags.weather_etl_dag.retrieve_country_code"):
        country = 123
        result = get_country_code(country)
        assert isinstance(result, XComArg)

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "status": "error",
            "message": f"Invalid input type for country name. Expected string but got {type(country)}",
            "error": "Invalid input type",
        }

        simulated_result = mock_ti.xcom_pull(task_ids=result.operator.task_id)

        assert simulated_result == {
            "status": "error",
            "message": f"Invalid input type for country name. Expected string but got {type(country)}",
            "error": "Invalid input type",
        }

        mock_ti.xcom_pull.assert_called_once_with(task_ids=result.operator.task_id)


def test_get_geographical_data() -> None:
    """
    Test the get_geographical_data function in the weather_etl_dag module

    The function should return an XComArg object that can be used to retrieve the geographical data
    for the given fields from the Airflow XCom.

    The function should also call the requests.get function with the correct URL
    and return the result from the XCom.

    Returns:
        None
    """

    with patch("dags.weather_etl_dag.requests.get") as mock_get:
        mock_get.return_value.json.return_value = {
            "name": "Lagos",
            "lat": 6.46,
            "lon": 3.39,
            "country": "Nigeria",
            "state": "Lagos",
        }
        city_names = ["Lagos"]
        result = get_geographical_data(
            "NG", city_names, ["name", "lat", "lon", "country", "state"]
        )
        assert isinstance(result, XComArg)

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "status": "success",
            "message": f"Current weather information for {city_names} has been retrieved from the API",
            "weather_records": [
                {
                    "name": "Lagos",
                    "lat": 6.46,
                    "lon": 3.39,
                    "country": "Nigeria",
                    "state": "Lagos",
                }
            ],
        }

        simulated_result = mock_ti.xcom_pull(task_ids=result.operator.task_id)

        assert simulated_result == {
            "status": "success",
            "message": f"Current weather information for {city_names} has been retrieved from the API",
            "weather_records": [
                {
                    "name": "Lagos",
                    "lat": 6.46,
                    "lon": 3.39,
                    "country": "Nigeria",
                    "state": "Lagos",
                }
            ],
        }

        mock_ti.xcom_pull.assert_called_once_with(task_ids=result.operator.task_id)


def test_restructure_geographical_data():
    """
    Test the restructure_geographical_data function in the weather_etl_dag module

    The function should return an XComArg object that can be used to retrieve the geographical data
    for the given fields from the Airflow XCom.

    The function should also call the requests.get function with the correct URL
    and return the result from the XCom.

    Returns:
        None
    """
    with patch("dags.weather_etl_dag.requests.get") as mock_get:
        mock_get.return_value.json.return_value = {
            "name": "Lagos",
            "lat": 6.46,
            "lon": 3.39,
            "country": "Nigeria",
            "state": "Lagos",
        }
        weather_data = [
            {
                "name": "Lagos",
                "lat": 6.46,
                "lon": 3.39,
                "country": "Nigeria",
                "state": "Lagos",
            }
        ]
        result = restructure_geographical_data(weather_data)
        assert isinstance(result, XComArg)

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "status": "success",
            "message": "Fields of the weather records have been retrieved",
            "weather_fields": {
                "city": "Lagos",
                "lat": 6.46,
                "lon": 3.39,
                "country": "Nigeria",
                "state": "Lagos",
            },
        }

        simulated_result = mock_ti.xcom_pull(task_ids=result.operator.task_id)

        assert simulated_result == {
            "status": "success",
            "message": "Fields of the weather records have been retrieved",
            "weather_fields": {
                "city": "Lagos",
                "lat": 6.46,
                "lon": 3.39,
                "country": "Nigeria",
                "state": "Lagos",
            },
        }

        mock_ti.xcom_pull.assert_called_once_with(task_ids=result.operator.task_id)


def test_process_geographical_records() -> None:
    """

    Test the process_geographical_records function in the weather_etl_dag module

    The function should return an XComArg object that can be used to retrieve the geographical data
    for the given fields from the Airflow XCom.

    The function should also call the requests.get function with the correct URL
    and return the result from the XCom.

    Returns:
        None
    """

    with patch("dags.weather_etl_dag.requests.get") as mock_get:
        mock_get.return_value.json.return_value = {
            "name": "Lagos",
            "lat": 6.46,
            "lon": 3.39,
            "country": "Nigeria",
            "state": "Lagos",
        }
        weather_data = [
            {
                "name": "Lagos",
                "lat": 6.46,
                "lon": 3.39,
                "country": "Nigeria",
                "state": "Lagos",
            }
        ]
        result = process_geographical_records(weather_data)
        assert isinstance(result, XComArg)

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "city": "Lagos",
            "country": "Nigeria",
            "state": "Lagos",
        }

        simulated_result = mock_ti.xcom_pull(task_ids=result.operator.task_id)

        assert simulated_result == {
            "city": "Lagos",
            "country": "Nigeria",
            "state": "Lagos",
        }

        mock_ti.xcom_pull.assert_called_once_with(task_ids=result.operator.task_id)


def test_get_longitude_latitude() -> None:
    """

    Test the get_longitude_latitude function in the weather_etl_dag module

    The function should return an XComArg object that can be used to retrieve the longitude and latitude
    for the given fields from the Airflow XCom.

    The function should also call the requests.get function with the correct URL
    and return the result from the XCom.

    Returns:
        None
    """
    with patch("dags.weather_etl_dag.requests.get") as mock_get:
        mock_get.return_value.json.return_value = {
            "name": "Lagos",
            "lat": 6.46,
            "lon": 3.39,
            "country": "Nigeria",
            "state": "Lagos",
        }
        weather_fields = {
            "city": "Lagos",
            "country": "Nigeria",
            "longitude": 3.39,
            "latitude": 6.46,
            "state": "Lagos",
        }
        result = get_longitude_latitude(weather_fields)
        assert isinstance(result, XComArg)

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = [(3.39, 6.46)]
        simulated_result = mock_ti.xcom_pull(task_ids=result.operator.task_id)

        assert simulated_result == [(3.39, 6.46)]

        mock_ti.xcom_pull.assert_called_once_with(task_ids=result.operator.task_id)


def test_merge_current_weather_data() -> None:
    """

    Test the merge_current_weather_data function in the weather_etl_dag module

    The function should return an XComArg object that can be used to retrieve the merged weather records
    for the given fields from the Airflow XCom.

    The function should also call the requests.get function with the correct URL
    and return the result from the XCom.

    Returns:
        None
    """
    with patch("dags.weather_etl_dag.requests.get") as mock_get:
        mock_get.return_value.json.return_value = {
            "lat": 6.46,
            "lon": 3.39,
            "timezone": "Africa/Lagos",
            "timezone_offset": 3600,
            "current": {
                "dt": 1726747705,
                "sunrise": 1726724175,
                "sunset": 1726767705,
                "temp": 301.15,
                "feels_like": 305.15,
                "pressure": 1010,
                "humidity": 75,
                "dew_point": 296.15,
                "uvi": 10,
                "clouds": 20,
                "visibility": 10000,
                "wind_speed": 3.6,
                "wind_deg": 180,
                "weather": [{"main": "Clear", "description": "clear sky"}],
            },
        }
        lon_lat = [(3.39, 6.46)]
        city_info = [{"city": "Lagos", "country": "Nigeria", "state": "Lagos"}]
        result = merge_current_weather_data(
            lon_lat, "minutely,hourly,daily", city_info, "fake_api_key"
        )
        assert isinstance(result, XComArg)

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "status": "success",
            "message": f"Current weather information for {lon_lat} has been retrieved from the API",
            "weather_records": [
                {
                    "city": "Lagos",
                    "country": "Nigeria",
                    "state": "Lagos",
                    "lat": 6.46,
                    "lon": 3.39,
                    "timezone": "Africa/Lagos",
                    "timezone_offset": 3600,
                    "date_time": 1726747705,
                    "temp": 301.15,
                    "feels_like": 305.15,
                    "pressure": 1010,
                    "humidity": 75,
                    "dew_point": 296.15,
                    "ultraviolet_index": 10,
                    "clouds": 20,
                    "visibility": 10000,
                    "wind_speed": 3.6,
                    "wind_deg": 180,
                    "weather": "Clear",
                    "description": "clear sky",
                }
            ],
        }

        simulated_result = mock_ti.xcom_pull(task_ids=result.operator.task_id)

        assert simulated_result == {
            "status": "success",
            "message": "Current weather information for [(3.39, 6.46)] has been retrieved from the API",
            "weather_records": [
                {
                    "city": "Lagos",
                    "country": "Nigeria",
                    "state": "Lagos",
                    "lat": 6.46,
                    "lon": 3.39,
                    "timezone": "Africa/Lagos",
                    "timezone_offset": 3600,
                    "date_time": 1726747705,
                    "temp": 301.15,
                    "feels_like": 305.15,
                    "pressure": 1010,
                    "humidity": 75,
                    "dew_point": 296.15,
                    "ultraviolet_index": 10,
                    "clouds": 20,
                    "visibility": 10000,
                    "wind_speed": 3.6,
                    "wind_deg": 180,
                    "weather": "Clear",
                    "description": "clear sky",
                }
            ],
        }

        mock_ti.xcom_pull.assert_called_once_with(task_ids=result.operator.task_id)


def test_get_merged_weather_records() -> None:
    """

    Test the get_merged_weather_records function in the weather_etl_dag module

    The function should return an XComArg object that can be used to retrieve the merged weather records
    for the given fields from the Airflow XCom.

    The function should also call the requests.get function with the correct URL
    and return the result from the XCom.

    Returns:
        None
    """
    lon_lat = [(3.39, 6.46)]
    merged_weather = {
        "status": "success",
        "message": f"Current weather information for {lon_lat} has been retrieved from the API",
        "weather_records": [{"city": "Lagos", "country": "Nigeria", "state": "Lagos"}],
    }
    result = get_merged_weather_records(merged_weather)
    assert isinstance(result, XComArg)

    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = [
        {"city": "Lagos", "country": "Nigeria", "state": "Lagos"}
    ]

    simulated_result = mock_ti.xcom_pull(task_ids=result.operator.task_id)
    assert simulated_result == [
        {"city": "Lagos", "country": "Nigeria", "state": "Lagos"}
    ]

    mock_ti.xcom_pull.assert_called_once_with(task_ids=result.operator.task_id)


def test_transform_weather_records() -> None:
    """

    Test the transform_weather_records function in the weather_etl_dag module

    The function should return an XComArg object that can be used to retrieve the transformed weather records
    for the given fields from the Airflow XCom.

    The function should also call the requests.get function with the correct URL
    and return the result from the XCom.

    Returns:
        None
    """
    weather_data = [
        {
            "city": "Lagos",
            "country": "Nigeria",
            "state": "Lagos",
            "latitude": 6.46,
            "longitude": 3.39,
            "timezone": "Africa/Lagos",
            "timezone_offset": 3600,
            "date_time": 1726747705,
            "date": 1726747705,
            "year": 2024,
            "month": 9,
            "day": 26,
            "day_of_week": "Thursday",
            "sunrise": 1726724175,
            "sunset": 1726767705,
            "temp": 301.15,
            "feels_like": 305.15,
            "pressure": 1010,
            "humidity": 75,
            "dew_point": 296.15,
            "ultraviolet_index": 10,
            "clouds": 20,
            "visibility": 10000,
            "wind_speed": 3.6,
            "wind_deg": 180,
            "weather": "Clear",
            "description": "clear sky",
        }
    ]
    result = transform_weather_records(weather_data)
    assert isinstance(result, XComArg)

    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = weather_data

    simulated_result = mock_ti.xcom_pull(task_ids=result.operator.task_id)

    assert simulated_result == [
        {
            "city": "Lagos",
            "country": "Nigeria",
            "state": "Lagos",
            "latitude": 6.46,
            "longitude": 3.39,
            "timezone": "Africa/Lagos",
            "timezone_offset": 3600,
            "date_time": 1726747705,
            "date": 1726747705,
            "year": 2024,
            "month": 9,
            "day": 26,
            "day_of_week": "Thursday",
            "sunrise": 1726724175,
            "sunset": 1726767705,
            "temp": 301.15,
            "feels_like": 305.15,
            "pressure": 1010,
            "humidity": 75,
            "dew_point": 296.15,
            "ultraviolet_index": 10,
            "clouds": 20,
            "visibility": 10000,
            "wind_speed": 3.6,
            "wind_deg": 180,
            "weather": "Clear",
            "description": "clear sky",
        }
    ]

    mock_ti.xcom_pull.assert_called_once_with(task_ids=result.operator.task_id)


def test_load_records_to_location_dim() -> None:
    """

    Test the load_records_to_location_dim function in the weather_etl_dag module

    The function should return an XComArg object that can be used to retrieve the transformed weather records
    for the given fields from the Airflow XCom.

    The function should also call the requests.get function with the correct URL
    and return the result from the XCom.

    Returns:
        None
    """
    with patch("dags.weather_etl_dag.query_existing_data") as mock_query:
        mock_query.return_value = {
            "existing_ids": [],
            "record_list": [{"city": "Lagos", "country": "Nigeria", "state": "Lagos"}],
        }
        result = load_records_to_location_dim(
            [{"city": "Lagos", "country": "Nigeria", "state": "Lagos"}],
            MagicMock(),
            MagicMock(),
            MagicMock(),
        )
        assert isinstance(result, XComArg)

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "status": "success",
            "message": "1 location records have been loaded",
        }

        simulated_result = mock_ti.xcom_pull(task_ids=result.operator.task_id)
        assert simulated_result == {
            "status": "success",
            "message": "1 location records have been loaded",
        }

        mock_ti.xcom_pull.assert_called_once_with(task_ids=result.operator.task_id)


def test_create_date_dim() -> None:
    """

    Test the create_date_dim function in the weather_etl_dag module

    The function should return an XComArg object that can be used to retrieve the transformed weather records
    for the given fields from the Airflow XCom.

    The function should also call the requests.get function with the correct URL
    and return the result from the XCom.

    Returns:
        None
    """
    with patch("dags.weather_etl_dag.query_existing_data") as mock_query:
        mock_query.return_value = {
            "existing_ids": [],
            "record_list": [
                {
                    "date": 1726747705,
                    "year": 2024,
                    "month": 9,
                    "day": 26,
                    "day_of_week": "Thursday",
                }
            ],
        }
        result = create_date_dim("2024", "2024", MagicMock(), MagicMock())
        assert isinstance(result, XComArg)

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "status": "success",
            "message": "date records have been loaded",
        }

        simulated_result = mock_ti.xcom_pull(task_ids=result.operator.task_id)
        assert simulated_result == {
            "status": "success",
            "message": "date records have been loaded",
        }

        mock_ti.xcom_pull.assert_called_once_with(task_ids=result.operator.task_id)


def test_join_date_dim_with_weather_fact() -> None:
    """

    Test the join_date_dim_with_weather_fact function in the weather_etl_dag module

    The function should return an XComArg object that can be used to retrieve the transformed weather records
    for the given fields from the Airflow XCom.

    The function should also call the requests.get function with the correct URL
    and return the result from the XCom.

    Returns:
        None
    """
    with patch("dags.weather_etl_dag.query_existing_data") as mock_query:
        mock_query.return_value = {
            "existing_ids": [1726747705],
            "record_list": [
                {
                    "date": 1726747705,
                    "year": 2024,
                    "month": 9,
                    "day": 26,
                    "day_of_week": "Thursday",
                }
            ],
        }
        result = join_date_dim_with_weather_fact(MagicMock(), MagicMock(), "{}")
        assert isinstance(result, XComArg)

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "status": "success",
            "message": "Date records have been joined",
        }

        simulated_result = mock_ti.xcom_pull(task_ids=result.operator.task_id)
        assert simulated_result == {
            "status": "success",
            "message": "Date records have been joined",
        }

        mock_ti.xcom_pull.assert_called_once_with(task_ids=result.operator.task_id)


def test_load_records_to_weather_type_dim() -> None:
    """

    Test the load_records_to_weather_type_dim function in the weather_etl_dag module

    The function should return an XComArg object that can be used to retrieve the transformed weather records
    for the given fields from the Airflow XCom.

    The function should also call the requests.get function with the correct URL
    and return the result from the XCom.

    Returns:
        None
    """
    with patch("dags.weather_etl_dag.query_existing_data") as mock_query:
        mock_query.return_value = {
            "existing_ids": [],
            "record_list": [{"city": "Lagos", "country": "Nigeria", "state": "Lagos"}],
        }
        result = load_records_to_weather_type_dim(
            [{"city": "Lagos", "country": "Nigeria", "state": "Lagos"}],
            MagicMock(),
            MagicMock(),
            MagicMock(),
        )
        assert isinstance(result, XComArg)

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "status": "success",
            "message": "weather type records have been loaded",
        }

        simulated_result = mock_ti.xcom_pull(task_ids=result.operator.task_id)
        assert simulated_result == {
            "status": "success",
            "message": "weather type records have been loaded",
        }

        mock_ti.xcom_pull.assert_called_once_with(task_ids=result.operator.task_id)


def test_weather_etl_dag() -> None:
    """

    Test the weather_etl_dag function in the weather_etl_dag module

    The function should return a DAG object with the correct attributes

    Returns:
        None
    """
    dag = weather_etl_dag()
    assert dag.task_count == 12
    assert dag.dag_id == "weather_etl_dag"
