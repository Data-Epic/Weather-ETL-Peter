from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.decl_api import DeclarativeMeta
import requests
from dags.config import error_logger, logger
from typing import List, Dict, Union

def query_existing_data(model: declarative_base, 
                        data: list,
                        db: Session) -> dict:

    """
    Function to query existing data from the database

    Args:
    model (declarative_base): A sqlalchemy model
    db (Session): A sqlalchemy session object
    data (list): A list of dictionaries containing the data to be queried

    Returns:
    list: A dictionary containing the existing data, existing ids, record list and record ids

    """

    if isinstance(data, list) is True:
        if isinstance(db,Session) is True \
            and isinstance(model,DeclarativeMeta) is True:

            record_ids = []
            for record in data:
                record['id'] = data.index(record) + 1
                record_ids.append(record['id'])
            existing_data = db.query(model).filter(model.id.in_(record_ids)).all()
            existing_ids = [existing.id for existing in existing_data]
            record_list = [record for record in data]
            record_ids = [record['id'] for record in record_list]
            return {
                'existing_data': existing_data,
                'existing_ids': existing_ids,
                'record_list': record_list,
                'record_ids': record_ids
            }
        else:
            raise ValueError(
                "Invalid database session. Database session must be a sqlalchemy session object, and model must be a sqlalchemy model"
            )
    else:
        raise ValueError("Invalid data format. Data argument must be a list of dictionaries")


def retrieve_country_code(country: str) -> Dict[str, str]:

    """
    Function to retrieve the country code from the restcountries API
    Args:
    country (str): Name of the country

    Returns: country_code (str): A country code for the given country

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
        # print("data", data)
        country_code = data['cca2']

        logger.info({
            "status": "success",
            "message": f"Country code for {country} is {country_code}",
            "country_codes": country_code
        })
        return {
            "status": "success",
            "message": f"Country code for {country} is {country_code}",
            "country_codes": country_code
        }
        
    except Exception as e:
        error_logger.error({
            "status": "error",
            "message": f"Unable to get country code for {country} from the API",
            "error": str(e) 
            })
        return {
            "status": "error",
            "message": f"Unable to get country code for {country} from the API",
            "error": str(e) 
            }

def retrieve_country_codes( 
                           country_code_list: list,
                           country_name: str) -> dict:

    """
    Function to retrieve the country codes from a list of country names

    Args:
    countries (list): A list of country names

    Returns:
    dict: A dictionary containing the country names and their respective country codes

    """
    try:
        url = f"https://restcountries.com/v3.1/name/{country_name}"
        response = requests.get(url)
        data = response.json()[0]
        # print("data", data)
        country_code = data['cca2']
        logger.info({
            "status": "success",
            "message": f"Country code for {country_name} is {country_code}",
            "country_codes": country_code
        })
        return {
            "status": "success",
            "message": f"Country code for {country_name} is {country_code}",
            "country_codes": country_code
        }

    except Exception as e:
        error_logger.error({
            "status": "error",
            "message": f"Unable to get country code for {country_name} from the API",
            "error": str(e) 
            })
        return {
            "status": "error",
            "message": f"Unable to get country code for {country_name} from the API",
            "error": str(e) 
            }

def get_data_from_country_code(country_code: str,
                               city_name: str,
                               fields:list,
                               API_KEY:str) -> dict:

    """
    Function to retrieve data from a country code

    Args:
    country_code (str): A country code
    city_name (str): Name of the city
    fields (list): A list of fields to retrieve from the API
    API_KEY (str): An API key

    Returns:
    dict: A dictionary containing the country data

    """

    try:
        weather_dict = {}
        url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name},{country_code}&limit=1&appid={API_KEY}"
        response = requests.get(url)
    
    except Exception as e:
        error_logger.error({
            "status": "error",
            "message": f"Unable to get weather information for {city_name} from the API",
            "error": str(e)
        })
        return {
            "status": "error",
            "message": f"Unable to get weather information for {city_name} from the API",
            "error": str(e)
        }
    data = response.json()

    if data:
        data = data[0]
        for field in fields:
            weather_dict[field] = data.get(field)

    logger.info({
        "status": "success",
        "message": f"Weather information for {city_name} retrieved successfully",
        "weather_data": weather_dict
    })
    return {
        "status": "success",
        "message": f"Weather information for {city_name} retrieved successfully",
        "weather_data": weather_dict
    }