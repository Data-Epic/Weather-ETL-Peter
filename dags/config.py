import logging
import os
import sys

from airflow.models import Variable

from dags.logger_config import error_log_file_path, log_file_path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# set up logging
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

error_logger = logging.getLogger("error_logger")
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler(error_log_file_path)
error_logger.addHandler(error_handler)

logger_handler = logging.FileHandler(log_file_path)
logger = logging.getLogger("logger")
logger.addHandler(logger_handler)

API_KEY = os.getenv("API_KEY")
CITY_NAMES = os.getenv("CITY_NAMES")
FIELDS = os.getenv("FIELDS")
WEATHER_FIELDS_EXCLUDE = os.getenv("WEATHER_FIELDS_EXCLUDE")
DATABASE_URL = os.getenv("DATABASE_URL")
COUNTRY_NAMES = os.getenv("COUNTRY_NAMES")
START_DATE_YEAR = os.getenv("START_DATE_YEAR")
END_DATE_YEAR = os.getenv("END_DATE_YEAR")

AIRFLOW_API_KEY = Variable.get("API_KEY", default_var=API_KEY)
AIRFLOW_CITY_NAMES = Variable.get("CITY_NAMES", default_var=CITY_NAMES)
AIRFLOW_COUNTRY_NAMES = Variable.get("COUNTRY_NAMES", default_var=COUNTRY_NAMES)
AIRFLOW_FIELDS = Variable.get("FIELDS", default_var=FIELDS)
AIRFLOW_WEATHER_FIELDS_EXCLUDE = Variable.get(
    "WEATHER_FIELDS_EXCLUDE", default_var=WEATHER_FIELDS_EXCLUDE
)
AIRFLOW_DATABASE_URL = Variable.get("DATABASE_URL", default_var=DATABASE_URL)
AIRFLOW_START_DATE_YEAR = Variable.get("START_DATE_YEAR", default_var=START_DATE_YEAR)
AIRFLOW_END_DATE_YEAR = Variable.get("END_DATE_YEAR", default_var=END_DATE_YEAR)


def process_var(var: str) -> list:
    """
    Process the environmental variable from a string to a list of strings if
    there is a comma in the varable

    Args: var(str) An environmental variable.

    returns: var(list) (A list)
    """
    if isinstance(var, str) is True:
        if "," in var:
            return var.split(",")

        else:
            return var
    else:
        error_logger.error(
            {
                "status": "error",
                "error": "Invalid input. Please provide a string as argument",
            }
        )
        return {
            "status": "error",
            "error": "Invalid input. Please provide a string as argument",
        }


CITY_NAMES = process_var(CITY_NAMES)
FIELDS = process_var(FIELDS)
COUNTRY_NAMES = process_var(COUNTRY_NAMES)

AIRFLOW_CITY_NAMES = process_var(AIRFLOW_CITY_NAMES)
AIRFLOW_FIELDS = process_var(AIRFLOW_FIELDS)
AIRFLOW_COUNTRY_NAMES = process_var(AIRFLOW_COUNTRY_NAMES)
