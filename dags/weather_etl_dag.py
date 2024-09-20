import requests
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dags.database import get_db
from dags.models import Weather
from dags.utils import (query_existing_data, retrieve_country_code,
                         retrieve_country_codes, get_data_from_country_code)
from dags.config import (error_logger, logger,
                        AIRFLOW_FIELDS, AIRFLOW_CITY_NAMES, AIRFLOW_COUNTRY_NAMES,
                          AIRFLOW_WEATHER_FIELDS_EXCLUDE, AIRFLOW_API_KEY)
from datetime import datetime
from datetime import datetime, timedelta
from typing import List, Union, Dict
from airflow.decorators import dag, task
from airflow.models import Variable


@task()
def get_country_code(countries: 
                     Union[List[str], str] = AIRFLOW_COUNTRY_NAMES) -> Dict[str, Union[str, List[str]]]:
    
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
        country_code = retrieve_country_code(country)['country_codes']
        
    elif isinstance(countries, list):
        country_code_list = []
        for country_name in countries:
            country_name = country_name.capitalize()
            country_code = retrieve_country_codes(country_code_list,
                                                    country_name
                                                  )['country_codes']
            country_code_list.append(country_code)

        logger.info({
            "status": "success",
            "message": f"Country codes for {countries} are {country_code_list}",
            "country_codes": country_code_list
        })
        return {
            "status": "success",
            "message": f"Country codes for {countries} are {country_code_list}",
            "country_codes": country_code_list
        }
    
    else:
        error_logger.error({
            "status": "error",
            "message": f"Invalid input type for country name. Expected string but got {type(country)}",
            "error": "Invalid input type"
        })
        return {
            "status": "error",
            "message": f"Invalid input type for country name. Expected string but got {type(country)}",
            "error": "Invalid input type"
        }
             
@task()
def get_current_weather(country_codes: Union[list, str], 
                        city_names: List[str],
                        fields: list,
                        api_key: str = AIRFLOW_API_KEY) -> Dict[str, Union[str, List[dict]]]:
    """
    This function is used to get the current weather information of cities in a country
    by using the country code and city names in an API call

    Args:
        country_code(str): Country code of the country
        city_names(list): List of city names to get the weather for
        fields(list): List of fields to be retrieved from the API
        api_key(str): API key to access the API
    Returns:
        Dict[str, Union[str, List[dict]]]: Dictionary containing the current weather information for the specified cities, fields and country code.

    Example: {
    "status": "success",
    "message": "Current weather information for {city_names} has been retrieved from the API",
    "weather_records" : [ {'lat': 6.46, 'lon': 3.39,
                        'timezone': 'Africa/Lagos',
                        'timezone_offset': 3600,
                        'current': {'dt': 1726747705, 'sunrise': 1726724175, '......': '......' } 
    ]
    } 
    
    """
    if isinstance(city_names, List):
        city_names = [city.capitalize() for city in city_names]
        if isinstance(country_codes, str):
            country_code = country_codes
            response_list = []
            for city_name in city_names:
                weather_dict = get_data_from_country_code(country_code, city_name, fields)['weather_data']
                response_list.append(weather_dict)

            logger.info({
                "status": "success",
                "message": f"Current weather information for {city_names} has been retrieved from the API",
                "weather_records": response_list
            })
            

            return {
                "status": "success",
                "message": f"Current weather information for {city_names} has been retrieved from the API",
                "weather_records": response_list
            }
        elif isinstance(country_codes, List):
            response_list = []
            
            for country_code in country_codes:
                for city_name in city_names:
                    weather_dict = get_data_from_country_code(country_code, city_name, fields, api_key)['weather_data']
                    if weather_dict:
                        response_list.append(weather_dict)
            
                
            logger.info({
                "status": "success",
                "message": f"Current weather information for {city_names} has been retrieved from the API",
                "weather_records": response_list
            })

            return {
                "status": "success",
                "message": f"Current weather information for {city_names} has been retrieved from the API",
                "weather_records": response_list
            }

    else:
        error_logger.error({
            "status": "error",
            "message": f"Invalid input type for city names. Expected list but got {type(city_names)}",
            "error": "Invalid input type"
        })
        return {
            "status": "error",
            "message": f"Invalid input type for city names. Expected list but got {type(city_names)}",
            "error": "Invalid input type"
        }

@task()   
def retrieve_weather_fields(weather_records: List[dict]) -> Dict[str, Union[str, List[dict]]]:
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
            if list(record.keys()) != ['name', 'lat', 'lon', 'country', 'state']:
                error_logger.error({
                    "status": "error",
                    "message": f"Invalid keys in the weather records. Expected keys are ['name', 'lat', 'lon', 'country', 'state']",
                    "error": "Invalid keys"
                })
                return {
                    "status": "error",
                    "message": f"Invalid keys in the weather records. Expected keys are ['name', 'lat', 'lon', 'country', 'state']",
                    "error": f"Invalid keys: {list(record.keys())}"
                }
        
        lon_lat = [(round(record['lon'], 2), round(record['lat'], 2)) for record in weather_records]
        
        weather_fields = []
        for record in weather_records:
            print("record", record)
            fields_dict = {}
            fields_dict['city'] = record['name']
            fields_dict['country'] = record['country']
            fields_dict['state'] = record['state']
            weather_fields.append(fields_dict)
        
        weather_fields_dict = {
            "weather_fields": weather_fields,
            "lon_lat": lon_lat
        }

        logger.info({
            "status": "success",
            "message": f"Fields of the weather records have been retrieved",
            "weather_fields": weather_fields_dict
        })
        return {
            "status": "success",
            "message": f"Fields of the weather records have been retrieved",
            "weather_fields": weather_fields_dict
        }


    else:
        error_logger.error({
            "status": "error",
            "message": f"Invalid input type for weather records. Expected list but got {type(weather_records)}",
            "error": "Invalid input type"
        })
        return {
            "status": "error",
            "message": f"Invalid input type for weather records. Expected list but got {type(weather_records)}",
            "error": "Invalid input type"
        }

@task()
def get_weather_records(weather_fields_dict: Dict[str, Union[List[dict],
                                                              List[tuple]]]) -> List[dict]:
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
        weather_fields = weather_fields_dict['weather_fields']
        return weather_fields
    else:
        error_logger.error({
            "status": "error",
            "message": f"Invalid input type for weather fields. Expected dict but got {type(weather_fields_dict)}",
            "error": "Invalid input type"
        })
        return {
            "status": "error",
            "message": f"Invalid input type for weather fields. Expected dict but got {type(weather_fields_dict)}",
            "error": "Invalid input type"
        }

@task()
def get_long_lat(weather_fields: Dict[str, Union[str, List[dict]]]) -> List[tuple]:
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
        lon_lat = weather_fields['lon_lat']
        return lon_lat
    else:
        error_logger.error({
            "status": "error",
            "message": f"Invalid input type for weather fields. Expected dict but got {type(weather_fields)}",
            "error": "Invalid input type"
        })
        return {
            "status": "error",
            "message": f"Invalid input type for weather fields. Expected dict but got {type(weather_fields)}",
            "error": "Invalid input type"
        }

@task()
def merge_weather_data(
    lon_lat: List[tuple],
    excluded_fields: str,
    weather_fields: List[dict],
    api_key: str 
) -> List[dict]:
    
    """
    This function is used to get the current weather of cities in a country
    by using the longitude and latitude of the cities in an API call. 
    It also joins the weather information from the API with the country 
    and state information retrieved earlier

    Args:
        lon_lat(List[tuple]): List of tuples containing the longitude and latitude of the cities e.g. [(3.39, 6.46), (3.93, 7.38), (8.52, 11.96), (-0.21, 5.56)]
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

                #print("data", data)

                weather_dict = {}
                for key in data.keys():
                    weather_dict[key] = data[key] 
                
                weather_dict['city'] = weather_fields[record_counter]['city']
                weather_dict['country'] = weather_fields[record_counter]['country']
                weather_dict['state'] = weather_fields[record_counter]['state']
                record_counter += 1
                
                response_list.append(weather_dict)
            
            except Exception as e:
                error_logger.error({
                    "status": "error",
                    "message": f"Unable to get weather information for {lon_lat} from the API",
                    "error": str(e)
                })
                return {
                    "status": "error",
                    "message": f"Unable to get weather information for {lon_lat} from the API",
                    "error": str(e)
                }
        
        logger.info({
            "status": "success",
            "message": f"Current weather information for {lon_lat} has been retrieved from the API",
            "weather_records": response_list
        })
        return {
            "status": "success",
            "message": f"Current weather information for {lon_lat} has been retrieved from the API",
            "weather_records": response_list
        }
    else:
        error_logger.error({
            "status": "error",
            "message": f"Invalid input type for lon_lat. Expected list but got {type(lon_lat)}",
            "error": "Invalid input type"
        })
        return {
            "status": "error",
            "message": f"Invalid input type for lon_lat. Expected list but got {type(lon_lat)}",
            "error": "Invalid input type"
        }

@task()
def get_merged_weather_records(merged_weather: Dict[str, Union[str, List[dict]]]) -> List[dict]:
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
        weather_records = merged_weather['weather_records']
        return weather_records
    else:
        error_logger.error({
            "status": "error",
            "message": f"Invalid input type for merged weather records. Expected dict but got {type(merged_weather)}",
            "error": "Invalid input type"
        })
        return {
            "status": "error",
            "message": f"Invalid input type for merged weather records. Expected dict but got {type(merged_weather)}",
            "error": "Invalid input type"
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
    transformed_weather_records = []
    for record in weather_records:
        transformed_record = {}
        transformed_record['city'] = record['city']
        transformed_record['country'] = record['country']
        transformed_record['state'] = record['state']
        transformed_record['latitude'] = record['lat']
        transformed_record['longitude'] = record['lon']
        transformed_record['timezone'] = record['timezone']
        transformed_record['timezone_offset'] = record['timezone_offset']
        date_time = record['current']['dt']
        date_time = datetime.fromtimestamp(date_time).strftime('%Y-%m-%d %H:%M:%S')
        transformed_record['date_time'] = date_time
        transformed_record['sunrise'] = record['current']['sunrise']
        transformed_record['sunset'] = record['current']['sunset']
        transformed_record['temp'] = record['current']['temp']
        transformed_record['feels_like'] = record['current']['feels_like']
        transformed_record['pressure'] = record['current']['pressure']
        transformed_record['humidity'] = record['current']['humidity']
        transformed_record['dew_point'] = record['current']['dew_point']
        transformed_record['ultraviolet_index'] = record['current']['uvi']
        transformed_record['clouds'] = record['current']['clouds']
        transformed_record['visibility'] = record['current']['visibility']
        transformed_record['wind_speed'] = record['current']['wind_speed']
        transformed_record['wind_deg'] = record['current']['wind_deg']
        transformed_record['weather'] = record['current']['weather'][0]['main']
        transformed_record['description'] = record['current']['weather'][0]['description']
        transformed_weather_records.append(transformed_record)
    
    return transformed_weather_records

@task()
def load_records_to_database(weather_data: List[dict]):
    """
    This function is used to load the transformed weather records into the postgres database

    Args:
        data(List[dict]): List of dictionaries containing the transformed weather records
    """

    try:
        with get_db() as db:
            data = query_existing_data(Weather, weather_data, db)
            existing_ids = data['existing_ids']
            record_list = data['record_list']

            no_data = 0
            for record in record_list:
                if record['id'] not in existing_ids:
                    weather = Weather(
                        city=record['city'],
                        country=record['country'],
                        state=record['state'],
                        latitude=record['latitude'],
                        longitude=record['longitude'],
                        timezone=record['timezone'],
                        timezone_offset=record['timezone_offset'],
                        date_time=record['date_time'],
                        sunrise=record['sunrise'],
                        sunset=record['sunset'],
                        temperature=record['temp'],
                        feels_like=record['feels_like'],
                        pressure=record['pressure'],
                        humidity=record['humidity'],
                        dew_point=record['dew_point'],
                        ultraviolet_index=record['ultraviolet_index'],
                        clouds=record['clouds'],
                        visibility=record['visibility'],
                        wind_speed=record['wind_speed'],
                        wind_deg=record['wind_deg'],
                        weather=record['weather'],
                        description=record['description']
                    )
                    db.add(weather)
                    no_data += 1
            db.commit()
            print(f"{no_data} weather records have been loaded to the database")
            logger.info({
                "status": "success",
                "message": f"{no_data} weather records have been loaded to the database",
                "weather_records": record_list
            })
            return {
                "status": "success",
                "message": f"{no_data} weather records have been loaded to the database",
                "weather_records": record_list
            }
    except Exception as e:
        error_logger.error({
            "status": "error",
            "message": "Unable to load weather records to the database",
            "error": str(e)
        })
        return {
            "status": "error",
            "message": "Unable to load weather records to the database",
            "error": str(e)
        }

@dag(start_date=datetime(2024, 9, 19),
        schedule=timedelta(hours=1),
        description="Weather ETL DAG that fetches weather data from the OpenWeather API, transforms the data and loads it into a Postgres database, It runs every hour",
        catchup=False, tags=['weather'],
        max_active_runs=1, render_template_as_native_obj=True) 
def weather_etl_dag():
    get_country_codes = get_country_code()['country_codes']
    get_weather_info = get_current_weather(get_country_codes, AIRFLOW_CITY_NAMES, AIRFLOW_FIELDS)['weather_records']
    weather_fields_dict = retrieve_weather_fields(get_weather_info)['weather_fields']
    weather_fields_records = get_weather_records(weather_fields_dict)
    long_lat = get_long_lat(weather_fields_dict)
    merging_weather_data = merge_weather_data(
            long_lat, 
            AIRFLOW_WEATHER_FIELDS_EXCLUDE,
            weather_fields_records,
            AIRFLOW_API_KEY
        )
    merged_weather_records = get_merged_weather_records(merging_weather_data)
    transform_records = transform_weather_records(merged_weather_records)
    load_records_to_database(transform_records)

weather_dag_instance = weather_etl_dag()