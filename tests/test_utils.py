from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

from dags.models import Base, LocationDim, WeatherFact
from dags.utils import (
    gen_hash_key_datedim,
    gen_hash_key_location_dim,
    gen_hash_key_weather_type_dim,
    gen_hash_key_weatherfact,
    get_data_from_country_code,
    insert_data_to_fact_table,
    query_existing_data,
    retrieve_country_code,
    retrieve_country_codes,
    update_data_to_fact_table,
    update_weather_fact_with_weather_type_id,
)

engine = create_engine("sqlite:///:memory:")
SessionLocal = sessionmaker(bind=engine)


def test_gen_hash_key_weatherfact() -> None:
    """
    Tests the gen_hash_key_weatherfact function for a successful case

    Returns:
        None
    """
    result = gen_hash_key_weatherfact()
    assert result["status"] == "success"
    assert result["message"] == "Hash key generated successfully"
    assert isinstance(result["hash_key"], str)
    assert len(result["hash_key"]) == 64


def test_gen_hash_key_location_dim_success() -> None:
    """
    Tests the gen_hash_key_location_dim function for a successful case

    Returns:
        None
    """
    data = {"country": "USA", "state": "California", "city": "Los Angeles"}
    result = gen_hash_key_location_dim(data)
    assert result["status"] == "success"
    assert result["message"] == "Hash key generated successfully"
    assert isinstance(result["hash_key"], str)
    assert len(result["hash_key"]) == 64


def test_gen_hash_key_location_dim_case_insensitive() -> None:
    """
    Tests the gen_hash_key_location_dim function for case-insensitive input

    Returns:
        None
    """
    data1 = {"country": "USA", "state": "California", "city": "Los Angeles"}
    data2 = {"country": "usa", "state": "CALIFORNIA", "city": "los angeles"}
    result1 = gen_hash_key_location_dim(data1)
    result2 = gen_hash_key_location_dim(data2)
    assert result1["hash_key"] == result2["hash_key"]


def test_gen_hash_key_location_dim_invalid_input() -> None:
    """
    Tests the gen_hash_key_location_dim function with invalid input

    Returns:
        None
    """

    result = gen_hash_key_location_dim("invalid")
    assert result["status"] == "error"
    assert (
        result["message"]
        == "Invalid data format. Data argument of the location data must be a dictionary"
    )


def test_gen_hash_key_location_dim_missing_key() -> None:
    """
    Tests the gen_hash_key_location_dim function with missing keys

    Returns:
        None
    """
    data = {"country": "USA", "state": "California"}
    result = gen_hash_key_location_dim(data)
    assert result["status"] == "error"
    assert result["message"] == "Unable to generate hash key for location data"
    assert result["error"] == "'city'"


def test_gen_hash_key_datedim_success() -> None:
    """
    Tests the gen_hash_key_datedim function for a successful case

    Returns:
        None
    """
    data = {"date": "2023-05-22"}
    result = gen_hash_key_datedim(data)
    assert result["status"] == "success"
    assert result["message"] == "Hash key generated successfully"
    assert isinstance(result["hash_key"], str)
    assert len(result["hash_key"]) == 64


def test_gen_hash_key_datedim_invalid_input() -> None:
    """
    Tests the gen_hash_key_datedim function with invalid input

    Returns:
        None
    """
    result = gen_hash_key_datedim("invalid")
    assert result["status"] == "error"
    assert (
        result["message"]
        == "Invalid data format. Data argument for date must be a dictionary"
    )


def test_gen_hash_key_datedim_missing_key() -> None:
    """
    Tests the gen_hash_key_datedim function with missing keys

    Returns:
        None
    """
    data = {"wrong_key": "2023-05-22"}
    result = gen_hash_key_datedim(data)
    assert result["status"] == "error"
    assert result["message"] == "Unable to generate hash key for date data"
    assert result["error"] == "'date'"


def test_gen_hash_key_weather_type_dim_success() -> None:
    """
    Tests the gen_hash_key_weather_type_dim function for a successful case

    Returns:
        None
    """
    data = {"weather": "Sunny"}
    result = gen_hash_key_weather_type_dim(data)
    assert result["status"] == "success"
    assert result["message"] == "Hash key generated successfully"
    assert isinstance(result["hash_key"], str)
    assert len(result["hash_key"]) == 64


def test_gen_hash_key_weather_type_dim_case_insensitive() -> None:
    """
    Tests the gen_hash_key_weather_type_dim function for case-insensitive input

    Returns:
        None
    """
    data1 = {"weather": "Sunny"}
    data2 = {"weather": "SUNNY"}
    result1 = gen_hash_key_weather_type_dim(data1)
    result2 = gen_hash_key_weather_type_dim(data2)
    assert result1["hash_key"] == result2["hash_key"]


def test_gen_hash_key_weather_type_dim_invalid_input() -> None:
    """
    Tests the gen_hash_key_weather_type_dim function with invalid input

    Returns:
        None
    """
    result = gen_hash_key_weather_type_dim("invalid")
    assert result["status"] == "error"
    assert (
        result["message"]
        == "Invalid data format. Data argument for weather must be a dictionary"
    )


def test_gen_hash_key_weather_type_dim_missing_key() -> None:
    """
    Tests the gen_hash_key_weather_type_dim function with missing keys

    Returns:
        None
    """
    data = {"wrong_key": "Sunny"}
    result = gen_hash_key_weather_type_dim(data)
    assert result["status"] == "error"
    assert result["message"] == "Unable to generate hash key for weather data"
    assert result["error"] == "'weather'"


def test_hash_consistency() -> None:
    """
    Tests the consistency of the hash key generation function

    Returns:
        None
    """
    data = {"country": "USA", "state": "California", "city": "Los Angeles"}
    result1 = gen_hash_key_location_dim(data)
    result2 = gen_hash_key_location_dim(data)
    assert result1["hash_key"] == result2["hash_key"]


def test_hash_uniqueness() -> None:
    """
    Tests the uniqueness of the hash key generation function

    Returns:
        None
    """
    data1 = {"country": "USA", "state": "California", "city": "Los Angeles"}
    data2 = {"country": "USA", "state": "California", "city": "San Francisco"}
    result1 = gen_hash_key_location_dim(data1)
    result2 = gen_hash_key_location_dim(data2)
    assert result1["hash_key"] != result2["hash_key"]


def test_special_characters() -> None:
    """
    Tests the location dimension hash key generation function with special characters

    Returns:
        None
    """
    data = {"country": "USA", "state": "California", "city": "San Francisco!@#$%^&*()"}
    result = gen_hash_key_location_dim(data)
    assert result["status"] == "success"
    assert isinstance(result["hash_key"], str)
    assert len(result["hash_key"]) == 64


@pytest.fixture(scope="function")
def db_session():
    Base.metadata.create_all(engine)
    session = SessionLocal()
    yield session
    session.close()
    Base.metadata.drop_all(engine)


def test_query_existing_data_success(db_session: Session) -> None:
    """
    Tests the query_existing_data function with existing data in the database for a successful case

    Args:
        db_session (Session): SQLAlchemy session object

    Returns:
        None
    """

    location1id = gen_hash_key_location_dim(
        {"country": "usa", "state": "california", "city": "los angeles"}
    )["hash_key"]
    location2id = gen_hash_key_location_dim(
        {"country": "usa", "state": "new york", "city": "new york"}
    )["hash_key"]
    location1 = LocationDim(
        id=location1id,
        country="usa",
        state="california",
        city="los angeles",
        latitude=34.0522,
        longitude=-118.2437,
        timezone="America/Los_Angeles",
        timezone_offset=-25200,
    )
    location2 = LocationDim(
        id=location2id,
        country="usa",
        state="new york",
        city="new york",
        latitude=40.7128,
        longitude=-74.0060,
        timezone="America/New_York",
        timezone_offset=-14400,
    )
    db_session.add_all([location1, location2])
    db_session.commit()

    data = [
        {
            "country": "USA",
            "state": "California",
            "city": "Los Angeles",
            "latitude": 34.0522,
            "longitude": -118.2437,
            "timezone": "America/Los_Angeles",
            "timezone_offset": -25200,
        },
        {
            "country": "USA",
            "state": "New York",
            "city": "New York",
            "latitude": 40.7128,
            "longitude": -74.0060,
            "timezone": "America/New_York",
            "timezone_offset": -14400,
        },
        {
            "country": "Canada",
            "state": "Ontario",
            "city": "Toronto",
            "latitude": 43.65107,
            "longitude": -79.347015,
            "timezone": "America/Toronto",
            "timezone_offset": -14400,
        },
    ]

    result = query_existing_data(
        LocationDim, data, db_session, gen_hash_key_location_dim
    )

    assert len(result["existing_data"]) == 2
    assert len(result["existing_ids"]) == 2
    assert len(result["record_list"]) == 3
    assert len(result["record_ids"]) == 3


def test_query_existing_data_empty(db_session: Session) -> None:
    """
    Tests the query_existing_data function with no existing data in the database

    Args:
        db_session (Session): SQLAlchemy session object

    Returns:
        None
    """
    data = [{"country": "Canada", "state": "Ontario", "city": "Toronto"}]
    result = query_existing_data(
        LocationDim, data, db_session, gen_hash_key_location_dim
    )

    assert len(result["existing_data"]) == 0
    assert len(result["existing_ids"]) == 0
    assert len(result["record_list"]) == 1
    assert len(result["record_ids"]) == 1


def test_query_existing_data_invalid_input() -> None:
    """
    Tests the query_existing_data function with invalid input

    Returns:
        None
    """
    with pytest.raises(
        ValueError,
        match="Invalid data format. Data argument must be a list of dictionaries",
    ):
        query_existing_data(
            LocationDim, "invalid", SessionLocal(), gen_hash_key_location_dim
        )

    with pytest.raises(
        ValueError,
        match="Invalid database session. Database session must be a sqlalchemy session object, and model must be a sqlalchemy model",
    ):
        query_existing_data(LocationDim, [{}], "invalid", gen_hash_key_location_dim)


def test_update_weather_fact_with_weather_type_id_success(db_session) -> None:
    """
    Tests the update_weather_fact_with_weather_type_id function with existing data in the database for a successful case

    Args:
        db_session (Session): SQLAlchemy session object

    Returns:
        None
    """
    weather_fact = WeatherFact(
        id="hash1",
        weather="Sunny",
        temperature=25.5,
        feels_like=26.0,
        pressure=1013,
        humidity=60,
        dew_point=15.5,
        ultraviolet_index=7.2,
        clouds=20,
        visibility=10000,
        wind_speed=5.5,
        wind_deg=180,
        sunset=1621234567,
        sunrise=1621191234,
        date=datetime.strptime("2023-05-22", "%Y-%m-%d"),
    )
    db_session.add(weather_fact)
    db_session.commit()

    record = {
        "id": "weather_type_hash",
        "weather": "Sunny",
        "temperature": 25.5,
        "feels_like": 26.0,
        "pressure": 1013,
        "humidity": 60,
        "dew_point": 15.5,
        "ultraviolet_index": 7.2,
        "clouds": 20,
        "visibility": 10000,
        "wind_speed": 5.5,
        "wind_deg": 180,
        "sunset": 1621234567,
        "sunrise": 1621191234,
        "date": datetime.strptime("2023-05-22", "%Y-%m-%d"),
    }
    result = update_weather_fact_with_weather_type_id(WeatherFact, db_session, record)

    assert result["status"] == "success"
    assert result["message"] == "Weather type id updated successfully"

    updated_fact = db_session.query(WeatherFact).first()
    assert updated_fact.weather_type_id == "weather_type_hash"


def test_update_weather_fact_with_weather_type_id_no_existing(db_session) -> None:
    """
    Tests the update_weather_fact_with_weather_type_id function with no existing data in the database

    Args:
        db_session (Session): SQLAlchemy session object

    Returns:
        None
    """
    record = {"id": "weather_type_hash", "weather": "Sunny"}
    result = update_weather_fact_with_weather_type_id(WeatherFact, db_session, record)

    assert result["status"] == "success"
    assert result["message"] == "Weather type id updated successfully"


def test_update_weather_fact_with_weather_type_id_invalid_input() -> None:
    """
    Tests the update_weather_fact_with_weather_type_id function with invalid input

    Returns:
        None
    """
    with pytest.raises(ValueError, match="Invalid data format"):
        update_weather_fact_with_weather_type_id(WeatherFact, SessionLocal(), "invalid")

    with pytest.raises(ValueError, match="Invalid database session"):
        update_weather_fact_with_weather_type_id(WeatherFact, "invalid", {})


def test_insert_data_to_fact_table_success(db_session) -> None:
    """
    Tests the insert_data_to_fact_table function with valid input

    Args:
        db_session (Session): SQLAlchemy session object

    Returns:
        None
    """

    record = {
        "id": "location_hash",
        "temp": 25.5,
        "feels_like": 26.0,
        "pressure": 1013,
        "weather": "Sunny",
        "humidity": 60,
        "dew_point": 15.5,
        "ultraviolet_index": 7.2,
        "clouds": 20,
        "visibility": 10000,
        "wind_speed": 5.5,
        "wind_deg": 180,
        "sunset": 1621234567,
        "sunrise": 1621191234,
        "date": datetime.strptime("2023-05-22", "%Y-%m-%d"),
    }

    result = insert_data_to_fact_table(WeatherFact, db_session, record)

    assert result["status"] == "success"
    assert result["message"] == "Corresponding Fact record inserted successfully"

    inserted_fact = db_session.query(WeatherFact).first()
    assert inserted_fact is not None
    assert inserted_fact.temperature == 25.5
    assert inserted_fact.weather == "Sunny"


def test_insert_data_to_fact_table_invalid_input() -> None:
    """
    Tests the insert_data_to_fact_table function with invalid input

    Returns:
        None
    """
    with pytest.raises(
        ValueError, match="Invalid data format. Data argument must be a dictionary"
    ):
        insert_data_to_fact_table(WeatherFact, SessionLocal(), "invalid")

    with pytest.raises(
        ValueError,
        match="Invalid database session. Database session must be a sqlalchemy session object, and model must be a sqlalchemy model",
    ):
        insert_data_to_fact_table(WeatherFact, "invalid", {})


@patch("dags.utils.gen_hash_key_weatherfact")
def test_insert_data_to_fact_table_duplicate(mock_gen_hash, db_session) -> None:
    """
    Tests the insert_data_to_fact_table function with duplicate data

    Args:
        db_session (Session): SQLAlchemy session object
        mocker (MockFixture): Pytest mocker object

    Returns:
        None
    """

    mock_gen_hash.return_value = {"hash_key": "fixed_hash"}
    record1 = {
        "id": "location_hash1",
        "temp": 25.5,
        "feels_like": 26.0,
        "pressure": 1013,
        "weather": "Sunny",
        "humidity": 60,
        "dew_point": 15.5,
        "ultraviolet_index": 7.2,
        "clouds": 20,
        "visibility": 10000,
        "wind_speed": 5.5,
        "wind_deg": 180,
        "sunset": 1621234567,
        "sunrise": 1621191234,
        "date": datetime.strptime("2023-05-22", "%Y-%m-%d"),
    }

    record2 = {
        "id": "location_hash2",
        "temp": 30.0,
        "feels_like": 32.0,
        "pressure": 1010,
        "weather": "Clear",
        "humidity": 55,
        "dew_point": 18.0,
        "ultraviolet_index": 8.5,
        "clouds": 10,
        "visibility": 15000,
        "wind_speed": 3.0,
        "wind_deg": 90,
        "sunset": 1621238000,
        "sunrise": 1621195000,
        "date": datetime.strptime("2023-05-22", "%Y-%m-%d"),
    }

    result1 = insert_data_to_fact_table(WeatherFact, db_session, record1)
    assert result1["status"] == "success"

    result2 = insert_data_to_fact_table(WeatherFact, db_session, record2)
    assert result2["status"] == "error"
    assert "UNIQUE constraint failed" in result2["error"]


@patch("dags.utils.gen_hash_key_weatherfact")
def test_update_data_to_fact_table(mock_gen_hash, db_session) -> None:
    """
    Tests the update_data_to_fact_table function with valid input

    Args:
        db_session (Session): SQLAlchemy session object
        mocker (MockFixture): Pytest mocker object

    Returns:
        None
    """

    mock_gen_hash.return_value = {"hash_key": "update_hash"}
    initial_data = {
        "id": "location_hash",
        "temp": 25.5,
        "feels_like": 26.0,
        "pressure": 1013,
        "humidity": 60,
        "dew_point": 15.5,
        "ultraviolet_index": 7.2,
        "clouds": 20,
        "visibility": 10000,
        "wind_speed": 5.5,
        "wind_deg": 180,
        "sunset": 1621234567,
        "sunrise": 1621191234,
        "weather": "Sunny",
        "date": datetime.strptime("2023-05-22", "%Y-%m-%d"),
    }
    insert_data_to_fact_table(WeatherFact, db_session, initial_data)

    updated_data = initial_data.copy()
    updated_data["temp"] = 26.5
    updated_data["weather"] = "Partly Cloudy"

    data_to_update = (
        db_session.query(WeatherFact).filter_by(location_id="location_hash").first()
    )
    assert data_to_update is not None

    result = update_data_to_fact_table(
        WeatherFact, db_session, data_to_update, updated_data
    )
    assert result["status"] == "success"
    assert result["message"] == "Data updated successfully"

    updated_record = db_session.query(WeatherFact).filter_by(id="update_hash").first()
    assert updated_record.temperature == 26.5
    assert updated_record.weather == "Partly Cloudy"


@patch("requests.get")
def test_retrieve_country_code_success(mock_get) -> None:
    """
    Tests the retrieve_country_code function with a successful API call

    Args:
        mock_get (MagicMock): Pytest mocker object

    Returns:
        None

    """
    mock_response = MagicMock()
    mock_response.json.return_value = [{"cca2": "US"}]
    mock_get.return_value = mock_response

    result = retrieve_country_code("United States")
    assert result["status"] == "success"
    assert result["country_codes"] == "US"


@patch("requests.get")
def test_retrieve_country_code_failure(mock_get) -> None:
    """
    Tests the retrieve_country_code function with a failed API call

    Args:
        mock_get (MagicMock): Pytest mocker object

    Returns:
        None

    """
    mock_get.side_effect = Exception("API Error")

    result = retrieve_country_code("Nonexistent Country")
    assert result["status"] == "error"
    assert "Unable to get country code" in result["message"]


@patch("requests.get")
def test_retrieve_country_codes(mock_get) -> None:
    """
    Tests the retrieve_country_codes function with a successful API call

    Args:
        mock_get (MagicMock): Pytest mocker object

    Returns:
        None

    """
    mock_response = MagicMock()
    mock_response.json.return_value = [{"cca2": "FR"}]
    mock_get.return_value = mock_response

    result = retrieve_country_codes(["France", "Germany"], "France")
    assert result["status"] == "success"
    assert result["country_codes"] == "FR"


@patch("requests.get")
def test_get_data_from_country_code_success(mock_get) -> None:
    """
    Tests the get_data_from_country_code function with a successful API call

    Args:
        mock_get (MagicMock): Pytest mocker object

    Returns:
        None

    """

    mock_response = MagicMock()
    mock_response.json.return_value = [{"lat": 48.8566, "lon": 2.3522, "name": "Paris"}]
    mock_get.return_value = mock_response

    result = get_data_from_country_code(
        "FR", "Paris", ["lat", "lon", "name"], "dummy_api_key"
    )
    assert result["status"] == "success"
    assert result["weather_data"] == {"lat": 48.8566, "lon": 2.3522, "name": "Paris"}


@patch("requests.get")
def test_get_data_from_country_code_failure(mock_get) -> None:
    """
    Tests the get_data_from_country_code function with a failed API call

    Args:
        mock_get (MagicMock): Pytest mocker object

    Returns:
        None

    """
    mock_get.side_effect = Exception("API Error")

    result = get_data_from_country_code(
        "XX", "Nonexistent City", ["lat", "lon"], "dummy_api_key"
    )
    assert result["status"] == "error"
    assert "Unable to get weather information" in result["message"]


@patch("sqlalchemy.orm.Session.add")
@patch("sqlalchemy.orm.Session.commit")
def test_insert_data_to_fact_table_db_error(mock_commit, mock_add, db_session) -> None:
    """
    Tests the insert_data_to_fact_table function with a database error

    Args:
        mock_commit (MagicMock): Pytest mocker object
        mock_add (MagicMock): Pytest mocker object
        db_session (Session): SQLAlchemy session object

    Returns:
        None

    """
    mock_commit.side_effect = IntegrityError(None, None, None)

    record = {
        "id": "location_hash1",
        "temp": 25.5,
        "feels_like": 26.0,
        "pressure": 1013,
        "humidity": 60,
        "dew_point": 15.5,
        "ultraviolet_index": 7.2,
        "clouds": 20,
        "visibility": 10000,
        "wind_speed": 5.5,
        "wind_deg": 180,
        "sunset": 1621234567,
        "sunrise": 1621191234,
        "weather": "Sunny",
        "date": datetime.strptime("2023-05-22", "%Y-%m-%d"),
    }

    result = insert_data_to_fact_table(WeatherFact, db_session, record)
    assert result["status"] == "error"
    assert result["message"] == "Unable to insert record to the fact table"


def test_insert_data_to_fact_table_invalid_model() -> None:
    """
    Tests the insert_data_to_fact_table function with an invalid model type

    Returns:
        None
    """

    class InvalidModel:
        pass

    with pytest.raises(
        ValueError,
        match="Invalid database session. Database session must be a sqlalchemy session object, and model must be a sqlalchemy model",
    ):
        insert_data_to_fact_table(InvalidModel, MagicMock(), {})


def test_insert_data_to_fact_table_invalid_session() -> None:
    """
    Tests the insert_data_to_fact_table function with an invalid session type

    Returns:
        None
    """
    with pytest.raises(
        ValueError,
        match="Invalid database session. Database session must be a sqlalchemy session object, and model must be a sqlalchemy model",
    ):
        insert_data_to_fact_table(WeatherFact, "invalid_session", {})
