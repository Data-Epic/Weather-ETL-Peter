from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dags.models import Base, DateDim, LocationDim, WeatherFact, WeatherTypeDim


# Set up an in-memory SQLite database for testing
@pytest.fixture(scope="module")
def test_db():
    """
    Function to create an in-memory SQLite database for testing

    Yields:
    Session: A sqlalchemy session object

    """
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    yield session

    session.close()
    Base.metadata.drop_all(engine)


def test_location_dim_creation(test_db) -> None:
    """
    Test to create a location dimension record

    Args:
    test_db: A sqlalchemy session object

    Returns:
    None

    """
    location = LocationDim(
        id="loc1",
        city="Lagos",
        country="Nigeria",
        state="Lagos State",
        latitude=6.5244,
        longitude=3.3792,
        timezone="Africa/Lagos",
        timezone_offset=1,
    )
    test_db.add(location)
    test_db.commit()

    result = test_db.query(LocationDim).filter_by(id="loc1").first()
    assert result.city == "Lagos"
    assert result.country == "Nigeria"
    assert result.state == "Lagos State"
    assert result.latitude == 6.5244
    assert result.longitude == 3.3792
    assert result.timezone == "Africa/Lagos"
    assert result.timezone_offset == 1


def test_date_dim_creation(test_db) -> None:
    """
    Test to create a date dimension record

    Args:
    test_db: A sqlalchemy session object

    Returns:
    None
    """
    date_entry = DateDim(
        id="date1",
        date=datetime(2024, 9, 1),
        year=2024,
        month=9,
        day=1,
        day_of_week="Sunday",
    )
    test_db.add(date_entry)
    test_db.commit()

    result = test_db.query(DateDim).filter_by(id="date1").first()
    assert result.year == 2024
    assert result.month == 9
    assert result.day == 1
    assert result.day_of_week == "Sunday"


def test_weather_type_dim_creation(test_db) -> None:
    """
    Test to create a weather type dimension record

    Args:
    test_db: A sqlalchemy session object

    Returns:
    None
    """
    weather_type = WeatherTypeDim(
        id="weather1", weather="Clear", description="Clear sky"
    )
    test_db.add(weather_type)
    test_db.commit()

    result = test_db.query(WeatherTypeDim).filter_by(id="weather1").first()
    assert result.weather == "Clear"
    assert result.description == "Clear sky"


def test_weather_fact_creation(test_db) -> None:
    """
    Test to create a weather fact record

    Args:
    test_db: A sqlalchemy session object

    Returns:
    None
    """
    location = LocationDim(
        id="loc2",
        city="abuja",
        country="nigeria",
        state="fct",
        latitude=6.5244,
        longitude=3.3792,
        timezone="Africa/Lagos",
        timezone_offset=1,
    )
    date_entry = DateDim(
        id="date2",
        date=datetime(2024, 9, 2).date(),
        year=2024,
        month=9,
        day=2,
        day_of_week="Sunday",
    )
    weather_type = WeatherTypeDim(
        id="weather2", weather="Clear", description="Clear sky"
    )

    date_record = test_db.query(LocationDim).all()
    print("date_record", date_record)

    test_db.add(location)
    test_db.add(date_entry)
    test_db.add(weather_type)
    test_db.commit()

    weather_fact = WeatherFact(
        id="fact2",
        location_id="loc2",
        date_id="date2",
        weather_type_id="weather2",
        temperature=30,
        feels_like=32.0,
        pressure=1012,
        weather="Clear",
        humidity=60,
        dew_point=24.0,
        ultraviolet_index=5.0,
        clouds=0,
        date=datetime(2024, 9, 2).date(),
        visibility=10,
        wind_speed=5.0,
        wind_deg=180,
        sunrise=1609545600,
        sunset=1609588800,
    )
    test_db.add(weather_fact)
    test_db.commit()

    result = test_db.query(WeatherFact).filter_by(id="fact2").first()
    assert result.temperature == 30
    assert result.feels_like == 32.0
    assert result.weather == "Clear"
