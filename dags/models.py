import os
import sys

from sqlalchemy import Column, Date, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

Base = declarative_base()


class LocationDim(Base):
    __tablename__ = "location_dim"

    id: str = Column(String(64), primary_key=True, index=True)
    city: str = Column(String(50), nullable=False)
    country: str = Column(String(50), nullable=False)
    state: str = Column(String(50), nullable=False)
    latitude: float = Column(Float(10), nullable=False)
    longitude: float = Column(Float(10), nullable=False)
    timezone: str = Column(String(50), nullable=False)
    timezone_offset: int = Column(Integer, nullable=False)
    created_at: DateTime = Column(DateTime(timezone=True), server_default=func.now())
    updated_at: DateTime = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return (
            f"Location(city={self.city}, country={self.country}, state={self.state}, "
            f"latitude={self.latitude}, longitude={self.longitude}, timezone={self.timezone}, "
            f"timezone_offset={self.timezone_offset}, created_at={self.created_at}, "
            f"updated_at={self.updated_at})"
        )


class DateDim(Base):
    __tablename__ = "date_dim"

    id: str = Column(String(64), primary_key=True, index=True)
    date: str = Column(Date, nullable=False)
    year: int = Column(Integer, nullable=False)
    month: int = Column(Integer, nullable=False)
    day: int = Column(Integer, nullable=False)
    day_of_week: str = Column(String(50), nullable=False)
    created_at: DateTime = Column(DateTime(timezone=True), server_default=func.now())
    updated_at: DateTime = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return (
            f"Date(date_time={self.date}, year={self.year}, month={self.month}, "
            f"day={self.day}, day_of_week={self.day_of_week}, created_at={self.created_at}, "
            f"updated_at={self.updated_at})"
        )


class WeatherTypeDim(Base):
    __tablename__ = "weather_type_dim"

    id: str = Column(String(64), primary_key=True, index=True)
    weather: str = Column(String(50), nullable=False)
    description: str = Column(String(50), nullable=False)
    created_at: DateTime = Column(DateTime(timezone=True), server_default=func.now())
    updated_at: DateTime = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return (
            f"WeatherType(weather={self.weather}, description={self.description}, "
            f"created_at={self.created_at}, updated_at={self.updated_at})"
        )


class WeatherFact(Base):
    __tablename__ = "weather_fact"

    id: str = Column(String(64), primary_key=True, index=True)
    location_id: str = Column(String(64), ForeignKey("location_dim.id"))
    date_id: str = Column(String(64), ForeignKey("date_dim.id"))
    weather_type_id: str = Column(String(64), ForeignKey("weather_type_dim.id"))
    temperature: int = Column(Integer, nullable=False)
    feels_like: float = Column(Float(10), nullable=False)
    pressure: int = Column(Integer, nullable=False)
    weather: str = Column(String(50), nullable=False)
    humidity: int = Column(Integer, nullable=False)
    dew_point: float = Column(Float(10), nullable=False)
    ultraviolet_index: float = Column(Float(10), nullable=False)
    clouds: int = Column(Integer, nullable=False)
    date: Date = Column(Date, nullable=False)
    visibility: int = Column(Integer, nullable=False)
    wind_speed: float = Column(Float(10), nullable=False)
    wind_deg: int = Column(Integer, nullable=False)
    sunrise: int = Column(Integer, nullable=False)
    sunset: int = Column(Integer, nullable=False)
    created_at: DateTime = Column(DateTime(timezone=True), server_default=func.now())
    updated_at: DateTime = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return (
            f"WeatherFact(location_id={self.location_id}, date_id={self.date_id}, "
            f"weather_type_id={self.weather_type_id}, temperature={self.temperature}, "
            f"feels_like={self.feels_like}, pressure={self.pressure}, "
            f"humidity={self.humidity}, dew_point={self.dew_point}, "
            f"ultraviolet_index={self.ultraviolet_index}, clouds={self.clouds}, "
            f"visibility={self.visibility}, wind_speed={self.wind_speed}, "
            f"wind_deg={self.wind_deg}, sunrise={self.sunrise}, sunset={self.sunset}, "
            f"created_at={self.created_at}, updated_at={self.updated_at})"
        )
