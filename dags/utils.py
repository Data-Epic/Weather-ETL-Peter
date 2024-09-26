import hashlib
import os
import random
import string
import sys
from typing import Dict, List, Union

import requests
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.sql import update

from dags.config import error_logger, logger
from dags.models import WeatherFact

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def gen_hash_key_weatherfact() -> Dict[str, str]:
    """
    Function to generate a hash key as a primary key for the weather fact table

    Returns:
    Dict[str,str]: A dictionary containing the status of the operation,
                     a message and the hash key

    Example: {
            "status": "success",
            "message": "Hash key generated successfully",
            "hash_key": "c3d8b3d9c4e"
    }
    """
    try:
        random_string = "".join(
            random.choices(string.ascii_letters + string.digits, k=20)
        )
        hash_key = hashlib.sha256(random_string.encode()).hexdigest()

        logger.info(
            {
                "status": "success",
                "message": "Hash key generated successfully",
                "hash_key": "hash_key",
            }
        )

        return {
            "status": "success",
            "message": "Hash key generated successfully",
            "hash_key": hash_key,
        }
    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to generate hash key for weather fact data",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to generate hash key for weather fact data",
            "error": str(e),
        }


def gen_hash_key_location_dim(data: dict) -> Dict[str, str]:

    """
    Function to generate a hash key for the location dimension table

    Args:
    data(dict): A dictionary containing the location data

    Returns:
    Dict[str,str]: A dictionary containing the status of the operation,
                     a message and the hash key

    Example: {
            "status": "success",
            "message": "Hash key generated successfully",
            "hash_key": "c3d8b3d9c4e"
    }
    """

    try:
        if isinstance(data, dict) is True:
            composite_key = f"{data['country']}_{data['state']}_{data['city']}"
            composite_key = composite_key.lower().replace(" ", "")
            hash_object = hashlib.sha256(composite_key.encode())
            hash_key = hash_object.hexdigest()

            logger.info(
                {
                    "status": "success",
                    "message": "Hash key generated successfully",
                    "hash_key": "hash_key",
                }
            )

            return {
                "status": "success",
                "message": "Hash key generated successfully",
                "hash_key": hash_key,
            }

        else:
            return {
                "status": "error",
                "message": "Invalid data format. Data argument of the location data must be a dictionary",
            }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to generate hash key for location data",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to generate hash key for location data",
            "error": str(e),
        }


def gen_hash_key_datedim(data: dict) -> Dict[str, str]:

    """
    Function to generate a hash key for the date data

    Args:
    data (dict): A dictionary containing the date dimension data

    Returns:
    Dict[str,str]: A dictionary containing the status of the operation,
                    a message and the hash key

    Example: {
            "status": "success",
            "message": "Hash key generated successfully",
            "hash_key": "c3d8b3d9
    }

    """
    try:
        if isinstance(data, dict) is True:
            composite_key = f"{data['date']}"
            composite_key = composite_key.lower().replace(" ", "")
            hash_object = hashlib.sha256(composite_key.encode())
            hash_key = hash_object.hexdigest()

            logger.info(
                {
                    "status": "success",
                    "message": "Hash key generated successfully",
                    "hash_key": "hash_key",
                }
            )

            return {
                "status": "success",
                "message": "Hash key generated successfully",
                "hash_key": hash_key,
            }

        else:
            return {
                "status": "error",
                "message": "Invalid data format. Data argument for date must be a dictionary",
            }
    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to generate hash key for date data",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to generate hash key for date data",
            "error": str(e),
        }


def gen_hash_key_weather_type_dim(data: dict) -> Dict[str, str]:

    """
    Function to generate a hash key for the weather type dimension data

    Args:
    data (dict): A dictionary containing the weather data

    Returns:
    Dict[str,str]: A dictionary containing the status of the operation,
                    a message and the hash key

    Example: {
            "status": "success",
            "message": "Hash key generated successfully",
            "hash_key": "c3d8b3d9
    }
    """
    try:
        if isinstance(data, dict) is True:
            composite_key = f"{data['weather']}"
            composite_key = composite_key.lower().replace(" ", "")
            hash_object = hashlib.sha256(composite_key.encode())
            hash_key = hash_object.hexdigest()

            logger.info(
                {
                    "status": "success",
                    "message": "Hash key generated successfully",
                    "hash_key": "hash_key",
                }
            )

            return {
                "status": "success",
                "message": "Hash key generated successfully",
                "hash_key": hash_key,
            }

        else:
            return {
                "status": "error",
                "message": "Invalid data format. Data argument for weather must be a dictionary",
            }
    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to generate hash key for weather data",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to generate hash key for weather data",
            "error": str(e),
        }


def query_existing_data(
    model: declarative_base,
    data: List[dict],
    db: Session,
    hash_function: Dict[str, str],
) -> Dict[str, Union[List[DeclarativeMeta], List[str], List[dict]]]:

    """
    Function to query existing data from the database

    Args:
    model (declarative_base): A sqlalchemy model
    db (Session): A sqlalchemy session object
    data (list): A list of dictionaries containing the data to be queried
    hash_function (dict): A dictionary containing the hash function to be used for the data

    Returns:
    Dict[str, Union[List[DeclarativeMeta], List[str], List[dict]]]: A dictionary containing the existing data,

    Example:
    >>> query_existing_data(LocationDim, data, db, gen_hash_key_location_dim)

    {"existing_data": [<LocationDim 1>, <LocationDim 2>],
    "existing_ids": [1, 2],
    "record_list": [{'country': 'Nigeria', 'state': 'Lagos', 'city': 'Ikeja'},
                    {'country': 'Nigeria', 'state': 'Lagos', 'city': 'Victoria Island'}],
    "record_ids": [1, 2]}
    """

    if isinstance(data, List) is True:
        if (
            isinstance(db, Session) is True
            and isinstance(model, DeclarativeMeta) is True
        ):

            record_ids = []
            for record in data:
                record_keys = list(record.keys())
                for key in record_keys:
                    if isinstance(record[key], str) is True:
                        record[key] = record[key].lower()
                hash_key = hash_function(record)
                hash_key = hash_key["hash_key"]
                record["id"] = hash_key
                record_ids.append(record["id"])

            existing_data = db.query(model).filter(model.id.in_(record_ids)).all()
            existing_ids = [existing.id for existing in existing_data]
            record_list = [record for record in data]
            record_ids = [record["id"] for record in record_list]
            return {
                "existing_data": existing_data,
                "existing_ids": existing_ids,
                "record_list": record_list,
                "record_ids": record_ids,
            }
        else:
            raise ValueError(
                "Invalid database session. Database session must be a sqlalchemy session object, and model must be a sqlalchemy model"
            )
    else:
        raise ValueError(
            "Invalid data format. Data argument must be a list of dictionaries"
        )


def update_weather_fact_with_weather_type_id(
    fact_model: declarative_base, db: Session, record: dict
) -> Dict[str, str]:

    """
    Function to update the weather fact table with the weather type id

    Args:
    fact_model (declarative_base): The fact sqlalchemy model to be updated
    db (Session): A sqlalchemy session object
    record (dict): A dictionary containing the data to be updated

    Returns:
    Dict[str,str]: A dictionary containing the status of the operation

    Example:
    >>> update_weather_fact_with_weather_type_id(WeatherFact, db, record)

    {"status": "success",
    "message": "Weather type id updated successfully"}

    """
    if isinstance(record, dict) is True:
        if (
            isinstance(db, Session) is True
            and isinstance(fact_model, DeclarativeMeta) is True
        ):

            try:
                weather_fact_to_update = (
                    db.query(fact_model)
                    .filter(fact_model.weather_type_id == record["id"])
                    .first()
                )

                if not weather_fact_to_update:
                    update_weather_type_id = (
                        update(fact_model)
                        .where(fact_model.weather == record["weather"])
                        .values(weather_type_id=record["id"])
                    )
                    db.execute(update_weather_type_id)
                    db.commit()
                else:
                    weather_fact_to_update.weather_type_id = record["id"]
                    db.add(weather_fact_to_update)
                    db.commit()

                return {
                    "status": "success",
                    "message": "Weather type id updated successfully",
                }

            except Exception as e:
                error_logger.error(
                    {
                        "status": "error",
                        "message": "Unable to insert weather type id to the fact table",
                        "error": str(e),
                    }
                )
                return {
                    "status": "error",
                    "message": "Unable to insert weather type id to the fact table",
                    "error": str(e),
                }
        else:
            raise ValueError(
                "Invalid database session. Database session must be a sqlalchemy session object, and model must be a sqlalchemy model"
            )
    else:
        raise ValueError("Invalid data format. Data argument must be a dictionary")


def insert_data_to_fact_table(
    model: declarative_base, db: Session, record: WeatherFact
) -> Dict[str, str]:

    """
    Function to insert data to the fact table

    Args:
    model (declarative_base): A sqlalchemy model
    db (Session): A sqlalchemy session object
    record (WeatherFact): A sqlalchemy model object in which the data will be inserted

    Returns:
    Dict[str,str]: A dictionary containing the status of the operation

    Example:
    >>> insert_data_to_fact_table(WeatherFact, db, record)

    {"status": "success",
    "message": "Corresponding Fact record inserted successfully"}


    """

    if isinstance(record, dict) is True:
        if (
            isinstance(db, Session) is True
            and isinstance(model, DeclarativeMeta) is True
        ):

            try:
                db_record = model(
                    id=gen_hash_key_weatherfact()["hash_key"],
                    location_id=record["id"],
                    temperature=record["temp"],
                    feels_like=record["feels_like"],
                    pressure=record["pressure"],
                    weather=record["weather"],
                    humidity=record["humidity"],
                    dew_point=record["dew_point"],
                    ultraviolet_index=record["ultraviolet_index"],
                    clouds=record["clouds"],
                    visibility=record["visibility"],
                    wind_speed=record["wind_speed"],
                    wind_deg=record["wind_deg"],
                    sunset=record["sunset"],
                    sunrise=record["sunrise"],
                    date=record["date"],
                )
                db.add(db_record)
                db.commit()
                return {
                    "status": "success",
                    "message": "Corresponding Fact record inserted successfully",
                }
            except Exception as e:
                db.rollback()
                error_logger.error(
                    {
                        "status": "error",
                        "message": "Unable to insert record to the fact table",
                        "error": str(e),
                    }
                )
                return {
                    "status": "error",
                    "message": "Unable to insert record to the fact table",
                    "error": str(e),
                }
        else:
            raise ValueError(
                "Invalid database session. Database session must be a sqlalchemy session object, and model must be a sqlalchemy model"
            )
    else:
        raise ValueError("Invalid data format. Data argument must be a dictionary")


def update_data_to_fact_table(
    model: declarative_base, db: Session, data_to_update: WeatherFact, record: dict
) -> Dict[str, str]:

    """
    Function to update data to the fact table

    Args:
    model (declarative_base): A sqlalchemy model
    db (Session): A sqlalchemy session object
    data_to_update (WeatherFact): A sqlalchemy model object containing the data to be updated
    record (dict): A dictionary containing the data to be updated

    Returns:
    dict: A dictionary containing the status of the operation

    Example:
    >>> update_data_to_fact_table(WeatherFact, db, data_to_update, record)

    {"status": "success",
    "message": "Data updated successfully"}


    """
    if isinstance(data_to_update, WeatherFact) is True:
        if (
            isinstance(db, Session) is True
            and isinstance(model, DeclarativeMeta) is True
        ):

            try:
                data_to_update.id = gen_hash_key_weatherfact()["hash_key"]
                data_to_update.location_id = record["id"]
                data_to_update.temperature = record["temp"]
                data_to_update.feels_like = record["feels_like"]
                data_to_update.pressure = record["pressure"]
                data_to_update.humidity = record["humidity"]
                data_to_update.dew_point = record["dew_point"]
                data_to_update.ultraviolet_index = record["ultraviolet_index"]
                data_to_update.clouds = record["clouds"]
                data_to_update.visibility = record["visibility"]
                data_to_update.wind_speed = record["wind_speed"]
                data_to_update.wind_deg = record["wind_deg"]
                data_to_update.sunset = record["sunset"]
                data_to_update.sunrise = record["sunrise"]
                data_to_update.weather = record["weather"]
                data_to_update.date = record["date"]
                db.add(data_to_update)
                db.commit()

                return {"status": "success", "message": "Data updated successfully"}
            except Exception as e:
                db.rollback()
                error_logger.error(
                    {
                        "status": "error",
                        "message": "Unable to update data to the fact table",
                        "error": str(e),
                    }
                )
                return {
                    "status": "error",
                    "message": "Unable to update data to the fact table",
                    "error": str(e),
                }
        else:
            raise ValueError(
                "Invalid database session. Database session must be a sqlalchemy session object, and model must be a sqlalchemy model"
            )
    else:
        raise ValueError(
            "Invalid data format. Data argument must be an isinstance of sqlalchemy model"
        )


def retrieve_country_code(country: str) -> Dict[str, str]:

    """
    Function to retrieve the country code from the restcountries API
    Args:
    country (str): Name of the country

    Returns:
    Dict[str, str]: A dictionary containing the status of the operation,
                    a message and the country code
    Examples:
    >>> retrieve_country_code("Nigeria")

    {"status": "success",
    "message": "Country code for Nigeria is NG",
    "country_codes": "NG"}

    """
    try:

        url = f"https://restcountries.com/v3.1/name/{country}"
        response = requests.get(url)
        data = response.json()[0]
        country_code = data["cca2"]

        logger.info(
            {
                "status": "success",
                "message": f"Country code for {country} is {country_code}",
                "country_codes": country_code,
            }
        )
        return {
            "status": "success",
            "message": f"Country code for {country} is {country_code}",
            "country_codes": country_code,
        }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": f"Unable to get country code for {country} from the API",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": f"Unable to get country code for {country} from the API",
            "error": str(e),
        }


def retrieve_country_codes(country_list: list, country_name: str) -> Dict[str, str]:

    """
    Function to retrieve the country codes from a list of country names

    Args:
    country_name (str): Name of the country

    Returns:
    Dict[str, str]: A dictionary containing the status of the operation,
                    a message and the country code
    Examples:
    >>> retrieve_country_codes("Nigeria")

    {"status": "success",
    "message": "Country code for Nigeria is NG",
    "country_codes": "NG"}

    """
    try:
        url = f"https://restcountries.com/v3.1/name/{country_name}"
        response = requests.get(url)
        data = response.json()[0]
        country_code = data["cca2"]
        logger.info(
            {
                "status": "success",
                "message": f"Country code for {country_name} is {country_code}",
                "country_codes": country_code,
            }
        )
        return {
            "status": "success",
            "message": f"Country code for {country_name} is {country_code}",
            "country_codes": country_code,
        }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": f"Unable to get country code for {country_name} from the API",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": f"Unable to get country code for {country_name} from the API",
            "error": str(e),
        }


def get_data_from_country_code(
    country_code: str, city_name: str, fields: list, API_KEY: str
) -> Dict[str, Union[str, Dict[str, str]]]:

    """
    Function to retrieve data from a country code

    Args:
    country_code (str): A country code
    city_name (str): Name of the city
    fields (list): A list of fields to retrieve from the API
    API_KEY (str): An API key

    Returns:
    Dict[str, Union[str, Dict[str, str]]]: A dictionary containing the status of the operation,
                                            a message and the weather data

    Example:
    >>> get_data_from_country_code("NG", "Lagos", ["lat", "lon", "name"], "API_KEY")

    {"status": "success",
    "message": "Weather information for Lagos retrieved successfully",
    "weather_data": {"lat": "6.5244", "lon": "3.3792", "name": "Lagos"}}

    """

    try:
        weather_dict = {}
        url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name},{country_code}&limit=1&appid={API_KEY}"
        response = requests.get(url)

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": f"Unable to get weather information for {city_name} from the API",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": f"Unable to get weather information for {city_name} from the API",
            "error": str(e),
        }
    data = response.json()

    if data:
        data = data[0]
        for field in fields:
            weather_dict[field] = data.get(field)

    logger.info(
        {
            "status": "success",
            "message": f"Weather information for {city_name} retrieved successfully",
            "weather_data": weather_dict,
        }
    )
    return {
        "status": "success",
        "message": f"Weather information for {city_name} retrieved successfully",
        "weather_data": weather_dict,
    }
