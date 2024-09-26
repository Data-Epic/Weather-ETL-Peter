import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Union

import pandas as pd
import requests
from airflow.decorators import dag, task
from sqlalchemy.ext.declarative import DeclarativeMeta

from dags.config import (
    AIRFLOW_API_KEY,
    AIRFLOW_CITY_NAMES,
    AIRFLOW_COUNTRY_NAMES,
    AIRFLOW_END_DATE_YEAR,
    AIRFLOW_FIELDS,
    AIRFLOW_START_DATE_YEAR,
    AIRFLOW_WEATHER_FIELDS_EXCLUDE,
    error_logger,
    logger,
)
from dags.database import get_db
from dags.models import DateDim, LocationDim, WeatherFact, WeatherTypeDim
from dags.utils import (
    gen_hash_key_datedim,
    gen_hash_key_location_dim,
    gen_hash_key_weather_type_dim,
    get_data_from_country_code,
    insert_data_to_fact_table,
    query_existing_data,
    retrieve_country_code,
    retrieve_country_codes,
    update_data_to_fact_table,
    update_weather_fact_with_weather_type_id,
)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@task()
def get_country_code(
    countries: Union[List[str], str] = AIRFLOW_COUNTRY_NAMES
) -> Dict[str, Union[str, List[str]]]:

    """
    This function is used to get the country code of the country(s) specified

    Args:
        countries (Union[List[str], str]): List of country names to get the country code for or a single country name
    Returns:
        Dict[str, Union[str, List[str]]]: Dictionary containing status, message, and country code(s)

    Example: {
        "status": "success",
        "message": "Country codes for Nigeria are NG",
        "country_codes": "NG"
    }
    """

    if isinstance(countries, str):
        country = countries.capitalize()
        country_code = retrieve_country_code(country)["country_codes"]

    elif isinstance(countries, list):
        country_code_list = []
        for country_name in countries:
            country_name = country_name.capitalize()
            country_code = retrieve_country_codes(country_code_list, country_name)[
                "country_codes"
            ]
            country_code_list.append(country_code)

        logger.info(
            {
                "status": "success",
                "message": f"Country codes for {countries} are {country_code_list}",
                "country_codes": country_code_list,
            }
        )
        return {
            "status": "success",
            "message": f"Country codes for {countries} are {country_code_list}",
            "country_codes": country_code_list,
        }

    else:
        error_logger.error(
            {
                "status": "error",
                "message": f"Invalid input type for country name. Expected string but got {type(country)}",
                "error": "Invalid input type",
            }
        )
        return {
            "status": "error",
            "message": f"Invalid input type for country name. Expected string but got {type(country)}",
            "error": "Invalid input type",
        }


@task()
def get_geographical_data(
    country_codes: Union[list, str],
    city_names: List[str],
    fields: list,
    api_key: str = AIRFLOW_API_KEY,
) -> Dict[str, Union[str, List[dict]]]:
    """
    This function is used to get the current weather information of cities in a country
    by using the country code and city names in an API call

    Args:
        country_codes (Union[list, str]): List of country codes or a single country code
        city_names (List[str]): List of city names to get the current weather information for
        fields (list): List of fields to be retrieved from the API
        api_key (str): API key to access the OpenWeather API
    Returns:
        Dict[str, Union[str, List[dict]]]: Dictionary containing the current weather information for the specified cities, fields and country code.

    Example: {
    "status": "success",
    "message": "Current weather information for {city_names} has been retrieved from the API",
    "weather_records" : [ {'lat': 6.46,
                            'lon': 3.39,
                          'country': 'Nigeria',
                            'state': 'Lagos',

                        }]

    """
    if isinstance(city_names, List):
        city_names = [city.capitalize() for city in city_names]
        if isinstance(country_codes, str):
            country_code = country_codes
            response_list = []
            for city_name in city_names:
                weather_dict = get_data_from_country_code(
                    country_code, city_name, fields
                )["weather_data"]
                response_list.append(weather_dict)

            logger.info(
                {
                    "status": "success",
                    "message": f"Current weather information for {city_names} has been retrieved from the API",
                    "weather_records": response_list,
                }
            )

            return {
                "status": "success",
                "message": f"Current weather information for {city_names} has been retrieved from the API",
                "weather_records": response_list,
            }
        elif isinstance(country_codes, List):
            response_list = []

            for country_code in country_codes:
                for city_name in city_names:
                    weather_dict = get_data_from_country_code(
                        country_code, city_name, fields, api_key
                    )["weather_data"]
                    if weather_dict:
                        response_list.append(weather_dict)

            logger.info(
                {
                    "status": "success",
                    "message": f"Current weather information for {city_names} has been retrieved from the API",
                    "weather_records": response_list,
                }
            )

            return {
                "status": "success",
                "message": f"Current weather information for {city_names} has been retrieved from the API",
                "weather_records": response_list,
            }

    else:
        error_logger.error(
            {
                "status": "error",
                "message": f"Invalid input type for city names. Expected list but got {type(city_names)}",
                "error": "Invalid input type",
            }
        )
        return {
            "status": "error",
            "message": f"Invalid input type for city names. Expected list but got {type(city_names)}",
            "error": "Invalid input type",
        }


@task()
def restructure_geographical_data(
    weather_records: List[dict],
) -> Dict[str, Union[str, List[dict]]]:
    """
    This function is used to retrieve the fields of the weather records;
    longitude and latitude of the cities
    city name, country and state of the cities

    Args:
        weather_records(List[dict]): List of dictionaries containing the weather records; city name, country, state
    Returns:
        Dict[str, Union[str, List[dict]]]: Dictionary containing the fields of the weather records such as;
          {city name, country, state}, {longitude, latitude} of each city

    Example: {
        "status": "success",
        "message": "Fields of the weather records have been retrieved",
        "weather_fields": {
            "weather_fields": [
                {'city': 'Lagos', 'country': 'Nigeria', 'state': 'Lagos'},
                {'city': 'Ibadan', 'country': 'Nigeria', 'state': 'Oyo'},
                {'city': 'Kano', 'country': 'Nigeria', 'state': 'Kano'},
                {'city': 'Accra', 'country': 'Ghana', 'state': 'Greater Accra'}
            ],
            "lon_lat": [(3.39, 6.46), (3.93, 7.38), (8.52, 11.96), (-0.21, 5.56)]
        }
    }
    """
    if isinstance(weather_records, List):
        for record in weather_records:
            if list(record.keys()) != ["name", "lat", "lon", "country", "state"]:
                error_logger.error(
                    {
                        "status": "error",
                        "message": "Invalid keys in the weather records. Expected keys are ['name', 'lat', 'lon', 'country', 'state']",
                        "error": "Invalid keys",
                    }
                )
                return {
                    "status": "error",
                    "message": "Invalid keys in the weather records. Expected keys are ['name', 'lat', 'lon', 'country', 'state']",
                    "error": f"Invalid keys: {list(record.keys())}",
                }

        lon_lat = [
            (round(record["lon"], 2), round(record["lat"], 2))
            for record in weather_records
        ]

        weather_fields = []
        for record in weather_records:
            print("record", record)
            fields_dict = {}
            fields_dict["city"] = record["name"]
            fields_dict["country"] = record["country"]
            fields_dict["state"] = record["state"]
            weather_fields.append(fields_dict)

        weather_fields_dict = {"weather_fields": weather_fields, "lon_lat": lon_lat}

        logger.info(
            {
                "status": "success",
                "message": "Fields of the weather records have been retrieved",
                "weather_fields": weather_fields_dict,
            }
        )
        return {
            "status": "success",
            "message": "Fields of the weather records have been retrieved",
            "weather_fields": weather_fields_dict,
        }

    else:
        error_logger.error(
            {
                "status": "error",
                "message": f"Invalid input type for weather records. Expected list but got {type(weather_records)}",
                "error": "Invalid input type",
            }
        )
        return {
            "status": "error",
            "message": f"Invalid input type for weather records. Expected list but got {type(weather_records)}",
            "error": "Invalid input type",
        }


@task()
def process_geographical_records(
    weather_fields_dict: Dict[str, Union[List[dict], List[tuple]]]
) -> List[dict]:
    """
    This function is used to get the weather records from the retrieve weather fields dictionary
    such as the city name, country, state, longitude and latitude of the cities

    Args:
        weather_fields_dict(Dict[str, Union[List[dict], List[tuple]]]): Dictionary containing the fields of the weather records such as;
          {city name, country, state}, {longitude, latitude} of each city
    Returns:
        List[dict]: List of dictionaries containing the weather information such as ;
         [state, city and country]

    Example:  [
                {'city': 'Lagos', 'country': 'Nigeria', 'state': 'Lagos'},
                {'city': 'Ibadan', 'country': 'Nigeria', 'state': 'Oyo'},
                {'city': 'Kano', 'country': 'Nigeria', 'state': 'Kano'},
                {'city': 'Accra', 'country': 'Ghana', 'state': 'Greater Accra'}
            ]
    """

    if isinstance(weather_fields_dict, Dict):
        weather_fields = weather_fields_dict["weather_fields"]
        return weather_fields
    else:
        error_logger.error(
            {
                "status": "error",
                "message": f"Invalid input type for weather fields. Expected dict but got {type(weather_fields_dict)}",
                "error": "Invalid input type",
            }
        )
        return {
            "status": "error",
            "message": f"Invalid input type for weather fields. Expected dict but got {type(weather_fields_dict)}",
            "error": "Invalid input type",
        }


@task()
def get_longitude_latitude(
    weather_fields: Dict[str, Union[str, List[dict]]]
) -> List[tuple]:
    """
    This function is used to get the longitude and latitude of the cities from the weather fields

    Args:
        weather_fields(Dict[str, Union[str, List[dict]]]): Dictionary containing the fields of the weather records such as;
          {city name, country, state}, {longitude, latitude} of each city
    Returns:
        List[tuple]: List of tuples containing the longitude and latitude of the cities

    Example: [(3.39, 6.46), (3.93, 7.38), (8.52, 11.96), (-0.21, 5.56)]
    """
    if isinstance(weather_fields, Dict):
        lon_lat = weather_fields["lon_lat"]
        return lon_lat
    else:
        error_logger.error(
            {
                "status": "error",
                "message": f"Invalid input type for weather fields. Expected dict but got {type(weather_fields)}",
                "error": "Invalid input type",
            }
        )
        return {
            "status": "error",
            "message": f"Invalid input type for weather fields. Expected dict but got {type(weather_fields)}",
            "error": "Invalid input type",
        }


@task()
def merge_current_weather_data(
    lon_lat: List[tuple], excluded_fields: str, weather_fields: List[dict], api_key: str
) -> List[dict]:

    """
    This function is used to get the current weather of cities in a country
    by using the longitude and latitude of the cities in an API call.
    It also joins the weather information from the API with the country
    and state information retrieved earlier

    Args:
        lon_lat(List[tuple]): List of tuples containing the longitude and latitude of the cities
        e.g. [(3.39, 6.46), (3.93, 7.38), (8.52, 11.96), (-0.21, 5.56)]
        excluded_fields(str): String containing the fields to be excluded from the API call e.g. 'minutely,hourly,daily'
        weather_fields(List[dict]): List of dictionaries containing names of country, state about the cities
    Returns:
        weather(dict): Dictionary containing the weather information,
        for the specified cities, fields and country code

    Example: {
        "status": "success",
        "message": "Current weather information for {lon_lat} has been retrieved from the API",
        "weather_records" : [ {'lat': 6.46, 'lon': 3.39,
                            'timezone': 'Africa/Lagos',
                            'timezone_offset': 3600,
                             'current': {'dt': 1726747705, 'sunrise': 1726724175
                            '......': '......'}
                            'city': 'Lagos',
                            'country': 'Nigeria',
                            'state': 'Lagos'
                            }
        ]

    """
    if isinstance(lon_lat, List) and isinstance(excluded_fields, str):
        response_list = []
        record_counter = 0
        for lon, lat in lon_lat:
            try:
                url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={excluded_fields}&appid={api_key}"
                response = requests.get(url)
                data = response.json()
                weather_dict = {}
                for key in data.keys():
                    weather_dict[key] = data[key]

                weather_dict["city"] = weather_fields[record_counter]["city"]
                weather_dict["country"] = weather_fields[record_counter]["country"]
                weather_dict["state"] = weather_fields[record_counter]["state"]
                record_counter += 1

                response_list.append(weather_dict)

            except Exception as e:
                error_logger.error(
                    {
                        "status": "error",
                        "message": f"Unable to get weather information for {lon_lat} from the API",
                        "error": str(e),
                    }
                )
                return {
                    "status": "error",
                    "message": f"Unable to get weather information for {lon_lat} from the API",
                    "error": str(e),
                }

        logger.info(
            {
                "status": "success",
                "message": f"Current weather information for {lon_lat} has been retrieved from the API",
                "weather_records": response_list,
            }
        )
        return {
            "status": "success",
            "message": f"Current weather information for {lon_lat} has been retrieved from the API",
            "weather_records": response_list,
        }
    else:
        error_logger.error(
            {
                "status": "error",
                "message": f"Invalid input type for lon_lat. Expected list but got {type(lon_lat)}",
                "error": "Invalid input type",
            }
        )
        return {
            "status": "error",
            "message": f"Invalid input type for lon_lat. Expected list but got {type(lon_lat)}",
            "error": "Invalid input type",
        }


@task()
def get_merged_weather_records(
    merged_weather: Dict[str, Union[str, List[dict]]]
) -> List[dict]:
    """
    This function is used to get the merged weather records from the merge weather data dictionary

    Args:
        merged_weather(Dict[str, Union[str, List[dict]]]): Dictionary containing the merged weather records
    Returns:
        List[dict]: List of dictionaries containing the merged weather records

    Example:  [
                {'lat': 6.46, 'lon': 3.39,
                'timezone': 'Africa/Lagos',
                'timezone_offset': 3600,
                'current': {'dt': 1726747705, 'sunrise': 1726724175
                '......': '......'}
                'city': 'Lagos',
                'country': 'Nigeria',
                'state': 'Lagos'
                }
            ]
    """
    if isinstance(merged_weather, Dict):
        weather_records = merged_weather["weather_records"]
        return weather_records
    else:
        error_logger.error(
            {
                "status": "error",
                "message": f"Invalid input type for merged weather records. Expected dict but got {type(merged_weather)}",
                "error": "Invalid input type",
            }
        )
        return {
            "status": "error",
            "message": f"Invalid input type for merged weather records. Expected dict but got {type(merged_weather)}",
            "error": "Invalid input type",
        }


@task()
def transform_weather_records(weather_records: List[dict]) -> List[dict]:
    """
    This function is used to transform the weather records into a
    a more structured list of dictionaries, with only the necessary fields.
    Each dictionary in the list contains the weather information for each city specified in the API call.
    The datetime field is also converted from a unix timestamp to a datetime object.

    Args:
        weather_records(List[dict]): List of dictionaries containing the weather records
    Returns:
        List[dict]: List of dictionaries containing the transformed weather records
    """

    if isinstance(weather_records, List):
        try:

            transformed_weather_records = []
            for record in weather_records:
                transformed_record = {}
                transformed_record["city"] = record["city"]
                transformed_record["country"] = record["country"]
                transformed_record["state"] = record["state"]
                transformed_record["latitude"] = record["lat"]
                transformed_record["longitude"] = record["lon"]
                transformed_record["timezone"] = record["timezone"]
                transformed_record["timezone_offset"] = record["timezone_offset"]
                date_time = record["current"]["dt"]
                date_time = datetime.fromtimestamp(date_time).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                date_time = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
                transformed_record["date_time"] = date_time
                transformed_record["date"] = date_time.date()
                transformed_record["year"] = transformed_record["date_time"].year
                transformed_record["month"] = transformed_record["date_time"].month
                transformed_record["day"] = transformed_record["date_time"].day
                transformed_record["day_of_week"] = transformed_record[
                    "date_time"
                ].strftime("%A")
                transformed_record["sunrise"] = record["current"]["sunrise"]
                transformed_record["sunset"] = record["current"]["sunset"]
                transformed_record["temp"] = record["current"]["temp"]
                transformed_record["feels_like"] = record["current"]["feels_like"]
                transformed_record["pressure"] = record["current"]["pressure"]
                transformed_record["humidity"] = record["current"]["humidity"]
                transformed_record["dew_point"] = record["current"]["dew_point"]
                transformed_record["ultraviolet_index"] = record["current"]["uvi"]
                transformed_record["clouds"] = record["current"]["clouds"]
                transformed_record["visibility"] = record["current"]["visibility"]
                transformed_record["wind_speed"] = record["current"]["wind_speed"]
                transformed_record["wind_deg"] = record["current"]["wind_deg"]
                transformed_record["weather"] = record["current"]["weather"][0]["main"]
                transformed_record["description"] = record["current"]["weather"][0][
                    "description"
                ]
                transformed_weather_records.append(transformed_record)

            logger.info(
                {
                    "status": "success",
                    "message": "Weather records have been transformed",
                    "weather_records": transformed_weather_records[:1],
                }
            )
            return transformed_weather_records

        except Exception as e:
            error_logger.error(
                {
                    "status": "error",
                    "message": "Unable to transform weather records",
                    "error": str(e),
                }
            )
            return {
                "status": "error",
                "message": "Unable to transform weather records",
                "error": str(e),
            }

    else:
        error_logger.error(
            {
                "status": "error",
                "message": f"Invalid input type for weather records. Expected list but got {type(weather_records)}",
                "error": "Invalid input type",
            }
        )
        return {
            "status": "error",
            "message": f"Invalid input type for weather records. Expected list but got {type(weather_records)}",
            "error": "Invalid input type",
        }


@task()
def load_records_to_location_dim(
    weather_data: List[dict],
    dim_model: DeclarativeMeta,
    fact_model: DeclarativeMeta,
    hash_function: Dict[str, str],
) -> Dict[str, Union[str, List[dict]]]:
    """
    This function is used to load the transformed weather records into the Location
    Dimension table in the postgres database

    Args:
        weather_data(List[dict]): List of dictionaries containing the transformed weather records
        dim_model(DeclarativeMeta): The Location Dimension model
        fact_model(DeclarativeMeta): The Weather Fact model
        hash_function(Dict[str, str]): The hash function to be used to generate the hash key for the records

    Returns:
        Dict[str, Union[str, List[dict]]]: Dictionary containing the status, message and weather records

    Examples:
    >>> load_records_to_location_dim(weather_data, LocationDim, WeatherFact, gen_hash_key_location_dim)
        {
            "status": "success",
            "message": "Location records have been loaded to the location dimension table",
            "weather_records": record_list
        }
    """
    try:
        with get_db() as db:
            data = query_existing_data(dim_model, weather_data, db, hash_function)
            existing_ids = data["existing_ids"]
            record_list = data["record_list"]
            no_data = 0
            new_records = []

            for record in record_list:
                if record["id"] not in existing_ids:
                    location = dim_model(
                        id=record["id"],
                        city=record["city"],
                        country=record["country"],
                        state=record["state"],
                        latitude=record["latitude"],
                        longitude=record["longitude"],
                        timezone=record["timezone"],
                        timezone_offset=record["timezone_offset"],
                    )
                    db.add(location)
                    no_data += 1
                    new_records.append(record)

                    # Check if the location record exists in the fact table
                    fact_record = (
                        db.query(fact_model)
                        .filter(fact_model.location_id == record["id"])
                        .first()
                    )
                    # If the record does not exist in the fact table, insert the record
                    if not fact_record:
                        inserted_data = insert_data_to_fact_table(
                            fact_model, db, record
                        )
                        if inserted_data["status"] != "success":
                            return {
                                "status": "error",
                                "message": "Unable to insert corresponding location fact record to the fact table",
                                "error": inserted_data["error"],
                            }
                    # If the record exists in the fact table, update the record
                    else:
                        updated_data = update_data_to_fact_table(fact_model, db, record)
                        if updated_data["status"] != "success":
                            return {
                                "status": "error",
                                "message": "Unable to update corresponding location fact record in the fact table",
                                "error": updated_data["error"],
                            }

                else:
                    data_to_update = (
                        db.query(dim_model).filter(dim_model.id == record["id"]).first()
                    )
                    data_to_update.latitude = record["latitude"]
                    data_to_update.longitude = record["longitude"]
                    data_to_update.timezone = record["timezone"]
                    data_to_update.timezone_offset = record["timezone_offset"]
                    db.add(data_to_update)
                    no_data += 1
                    new_records.append(record)

                    fact_to_update = (
                        db.query(fact_model)
                        .filter(fact_model.location_id == record["id"])
                        .first()
                    )
                    if not fact_to_update:
                        inserted_data = insert_data_to_fact_table(
                            fact_model, db, record
                        )
                        if inserted_data["status"] != "success":
                            return {
                                "status": "error",
                                "message": "Unable to insert corresponding location fact record to the fact table",
                                "error": inserted_data["error"],
                            }
                    else:
                        updated_data = update_data_to_fact_table(
                            fact_model, db, fact_to_update, record
                        )
                        if updated_data["status"] != "success":
                            return {
                                "status": "error",
                                "message": "Unable to update corresponding location fact record in the fact table",
                                "error": updated_data["error"],
                            }

            db.commit()
            logger.info(
                {
                    "status": "success",
                    "message": f"{no_data} location records have been loaded to the location dimension table",
                    "weather_records": new_records[:5],
                }
            )
            return {
                "status": "success",
                "message": f"{no_data} location records have been loaded to the location dimension table",
                "weather_records": record_list,
            }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to load weather location records to the location dimension table",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to load weather location records to the location dimension table",
            "error": str(e),
        }


@task()
def create_date_dim(
    start_year: str,
    end_year: str,
    model: DeclarativeMeta,
    hash_function: Dict[str, str],
) -> Dict[str, Union[str, List[dict]]]:
    """
    This function is used to load the transformed weather records into the Date Dimension table in the postgres database

    Args:
        start_year(str): The year to start creating date records from
        end_year(str): The year to stop creating date records
        model(DeclarativeMeta): The Date Dimension model
        hash_function(Dict[str, str]): The hash function to be used to generate the hash key for the records
    Returns:
        Dict[str, Union[str, List[dict]]]: Dictionary containing the status, message and weather records

    Examples:
    >>> create_date_dim('2020', '2021', DateDim, gen_hash_key_datedim)
        {
            "status": "success",
            "message": "Date records have been loaded to the date dimension table",
            "weather_records": record_list
        }
    """

    try:
        with get_db() as db:
            start_date = f"{start_year}-01-01"
            end_date = f"{end_year}-12-31"
            date_range = pd.date_range(start_date, end_date)
            date_records = []
            for date in date_range:
                record = {
                    "date": date,
                    "year": date.year,
                    "month": date.month,
                    "day": date.day,
                    "day_of_week": date.strftime("%A"),
                }
                date_records.append(record)

            data = query_existing_data(model, date_records, db, hash_function)
            existing_ids = data["existing_ids"]
            record_list = data["record_list"]
            no_data = 0
            new_records = []
            for record in record_list:
                if record["id"] not in existing_ids:
                    date = model(
                        id=record["id"],
                        date=record["date"],
                        year=record["year"],
                        month=record["month"],
                        day=record["day"],
                        day_of_week=record["day_of_week"],
                    )
                    db.add(date)
                    no_data += 1
                    new_records.append(record)

            db.commit()

            logger.info(
                {
                    "status": "success",
                    "message": f"{no_data} date records have been loaded to the date dimension table",
                    "weather_records": new_records[:1],
                }
            )
            return {
                "status": "success",
                "message": f"{no_data} date records have been loaded to the date dimension table",
                "weather_records": new_records[:1],
            }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to load weather date records to the date dimension table",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to load weather date records to the date dimension table",
            "error": str(e),
        }


@task()
def join_date_dim_with_weather_fact(
    fact_model: DeclarativeMeta, date_model: DeclarativeMeta, json_str: str
) -> Dict[str, Union[str, List[dict]]]:
    """
    This function is used to join the Date Dimension table with the Weather Fact table in the postgres database

    Args:
        fact_model(DeclarativeMeta): The Weather Fact model
        date_model(DeclarativeMeta): The Date Dimension model
        json_str(str): The JSON string containing the weather records
    Returns:
        Dict[str, Union[str, List[dict]]]: Dictionary containing the status, message and weather records

    Examples:
    >>> join_date_dim_with_weather_fact(WeatherFact, DateDim, json_str)
        {
            "status": "success",
            "message": "Date records have been joined with the weather fact table",
            "weather_records": record_list
        }
    """
    try:
        with get_db() as db:
            if json_str:
                weather_records = db.query(fact_model).all()
                date_records = db.query(date_model).all()
                if weather_records and date_records:
                    for record in weather_records:
                        for date in date_records:
                            if record.date == date.date:
                                record.date_id = date.id
                                db.add(record)
                    db.commit()
                    logger.info(
                        {
                            "status": "success",
                            "message": "Date records have been joined with the weather fact table",
                            "weather_records": weather_records[:1],
                        }
                    )
                    return {
                        "status": "success",
                        "message": "Date records have been joined with the weather fact table",
                        "weather_records": weather_records[:1],
                    }
                else:
                    error_logger.error(
                        {
                            "status": "error",
                            "message": "No records found in the date or weather fact table",
                            "error": "No records found",
                        }
                    )
                    return {
                        "status": "error",
                        "message": "No records found in the date or weather fact table",
                        "error": "No records found",
                    }
    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to join date records with the weather fact table",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to join date records with the weather fact table",
            "error": str(e),
        }


@task()
def load_records_to_weather_type_dim(
    weather_data: List[dict],
    model: DeclarativeMeta,
    fact_model: DeclarativeMeta,
    hash_function: Dict[str, str],
) -> Dict[str, Union[str, List[dict]]]:
    """
    This function is used to load the transformed weather records into the Weather Type Dimension table in the postgres database

    Args:
        weather_data(List[dict]): List of dictionaries containing the transformed weather records
        model(DeclarativeMeta): The Weather Type Dimension model
        fact_model(DeclarativeMeta): The Weather Fact model
        hash_function(Dict[str, str]): The hash function to be used to generate the hash key for the records

    Returns:
        Dict[str, Union[str, List[dict]]]: Dictionary containing the status, message and weather records

    Examples:
    >>> load_records_to_weather_type_dim(weather_data, WeatherTypeDim, WeatherFact, gen_hash_key_weather_type_dim)
        {
            "status": "success",
            "message": "Weather type records have been loaded to the weather type dimension table",
            "weather_records": record_list
        }
    """
    try:
        with get_db() as db:
            data = query_existing_data(model, weather_data, db, hash_function)
            existing_ids = data["existing_ids"]
            record_list = data["record_list"]

            no_data = 0
            new_records = []
            id_list = []

            for record in record_list:
                # if no data has been loaded to the weather type dimension table, load all data
                if len(id_list) == 0:
                    if record["id"] not in existing_ids:
                        weather_type = model(
                            id=record["id"],
                            weather=record["weather"],
                            description=record["description"],
                        )

                        db.add(weather_type)
                        db.commit()
                        id_list.append(record["id"])
                        no_data += 1

                        weather_fact_update = update_weather_fact_with_weather_type_id(
                            fact_model, db, record
                        )
                        if weather_fact_update["status"] != "success":
                            return {
                                "status": "error",
                                "message": "Unable to update weather fact with weather type id",
                                "error": weather_fact_update["error"],
                            }

                else:
                    # if data has been loaded already to the weather type dimension table, load only new data
                    if record["id"] not in id_list:
                        weather_type = model(
                            id=record["id"],
                            weather=record["weather"],
                            description=record["description"],
                        )
                        db.add(weather_type)
                        db.commit()
                        no_data += 1
                        id_list.append(record["id"])

                        weather_fact_update = update_weather_fact_with_weather_type_id(
                            fact_model, db, record
                        )
                        if weather_fact_update["status"] != "success":
                            return {
                                "status": "error",
                                "message": "Unable to update weather fact with weather type id",
                                "error": weather_fact_update["error"],
                            }

                    new_records.append(record)
                    db.refresh(weather_type)

            logger.info(
                {
                    "status": "success",
                    "message": f"{no_data} weather type records have been loaded to the weather type dimension table",
                    "weather_records": new_records[:1],
                }
            )
            return {
                "status": "success",
                "message": f"{no_data} weather type records have been loaded to the weather type dimension table",
                "weather_records": new_records[:1],
            }
    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to load weather type records to the weather type dimension table",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to load weather type records to the weather type dimension table",
            "error": str(e),
        }


@dag(
    start_date=datetime(2024, 9, 24),
    schedule_interval=timedelta(hours=1),
    description="Weather ETL DAG that fetches weather data from the OpenWeather API, transforms the data and loads it into a Postgres database",
    catchup=False,
    tags=["weather"],
    max_active_runs=1,
    render_template_as_native_obj=True,
)
def weather_etl_dag():
    """
    This function is used to create a Directed Acyclic Graph (DAG) for the Weather ETL process
    The DAG is used to fetch weather data from the OpenWeather API, transform the data and load it into a Postgres database
    """

    get_country_codes = get_country_code()["country_codes"]
    get_weather_info = get_geographical_data(
        get_country_codes, AIRFLOW_CITY_NAMES, AIRFLOW_FIELDS
    )["weather_records"]
    weather_fields_dict = restructure_geographical_data(get_weather_info)[
        "weather_fields"
    ]
    weather_fields_records = process_geographical_records(weather_fields_dict)
    long_lat = get_longitude_latitude(weather_fields_dict)
    merging_weather_data = merge_current_weather_data(
        long_lat,
        AIRFLOW_WEATHER_FIELDS_EXCLUDE,
        weather_fields_records,
        AIRFLOW_API_KEY,
    )

    merged_weather_records = get_merged_weather_records(merging_weather_data)
    transform_records = transform_weather_records(merged_weather_records)

    # task dependencies
    (
        load_records_to_location_dim(
            transform_records, LocationDim, WeatherFact, gen_hash_key_location_dim
        )
        >> load_records_to_weather_type_dim(
            transform_records,
            WeatherTypeDim,
            WeatherFact,
            gen_hash_key_weather_type_dim,
        )
        >> create_date_dim(
            AIRFLOW_START_DATE_YEAR,
            AIRFLOW_END_DATE_YEAR,
            DateDim,
            gen_hash_key_datedim,
        )
        >> join_date_dim_with_weather_fact(WeatherFact, DateDim, json_str="")
    )


weather_dag_instance = weather_etl_dag()
