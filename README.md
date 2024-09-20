# Weather-ETL-Peter
## Setting up Airflow with Docker: A Step-by-Step Guide

### Prerequisites:
* Docker and Docker Compose installed on your machine
* OpenWeatherMap API key (sign up at https://openweathermap.org/api)

Step 1: Project Setup
1. Clone the repository:
   ```
   git clone https://github.com/Data-Epic/Weather-ETL-Precious.git
   cd Weather-ETL-Precious
   ```

2. Create necessary directories:
   ```
   mkdir -p ./dags ./logs ./plugins ./config
   ```

3. Set up environment variables:
   ```
   echo -e "AIRFLOW_UID=$(id -u)" > .env
   ```

4. Edit the .env file and add the following variables:
   ```
   AIRFLOW_UID=your_uid
   DB_USER=myadmin
   DB_PASSWORD=mypassword
   DB_NAME=weather_etl
   DB_URL=postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@postgres/${DB_NAME}
   AIRFLOW__CORE__EXECUTOR=LocalExecutor
   AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@postgres/${DB_NAME}
   AIRFLOW_DATABASE_SQL_ALCHEMY_CONN=postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@postgres/${DB_NAME}
   AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
   AIRFLOW__CORE__LOAD_EXAMPLES=False
   AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT=400
   AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT=400
   AIRFLOW__TRIGGERER__DEFAULT_CAPACITY=1000
   API_KEY=your_weather_api_key
   ```

Step 2: Docker Compose File Explanation
Your docker-compose.yml file defines several services. Let's break them down:

1. postgres:
   - Uses PostgreSQL 13 image
   - Stores Airflow metadata and your project data
   - Configured with a health check to ensure it's ready before other services start

2. airflow-init:
   - Initializes the Airflow database and creates the first admin user
   - Depends on the postgres service being healthy

3. airflow-webserver:
   - Runs the Airflow web interface
   - Accessible at http://localhost:8080
   - Depends on airflow-init service completing successfully
   - Has a health check to ensure it's running before other services start

4. airflow-scheduler:
   - Monitors and triggers scheduled workflows
   - Depends on the airflow-webserver being healthy

5. airflow-triggerer:
   - Handles deferred task instances
   - Improves Airflow's ability to handle long-running tasks
   - Depends on the airflow-webserver being healthy

Step 3: Building and Starting the Services
1. Ensure you're in the project directory containing the docker-compose.yml file.

2. Build and start the Docker containers:
   ```
   docker-compose up --build
   ```

3. Wait for all services to start. You should see logs from each service in the console.

Step 4: Accessing Airflow
1. Once all services are running, open a web browser and go to http://localhost:8080

2. Log in with the default credentials:
   - Username: admin
   - Password: admin

Step 5: Using Airflow
1. The DAGs directory (./dags) is mounted to the Airflow containers. Place your DAG files here, and they will be automatically picked up by Airflow.

2. Logs are stored in the ./logs directory for easy access and debugging.

3. The PostgreSQL database is accessible on port 5432. You can connect to it using the credentials specified in your .env file.

Additional Notes:
- The scheduler service monitors your DAGs and triggers them based on their schedules or dependencies.
- The triggerer service helps manage long-running tasks and improves Airflow's scalability.
- You can customize Airflow configurations by modifying the environment variables in the .env file or docker-compose.yml.

Stopping the Services:
To stop all services, use:
```
docker-compose down
```

To stop services and remove volumes (this will delete all data):
```
docker-compose down -v
```

This setup provides a fully functional Airflow environment using Docker, with separate services for the database, webserver, scheduler, and triggerer, allowing for easy scaling and management of your weather ETL workflows.

Here's the optimized and grammatically corrected version of your step-by-step guide:

---

## Step-by-Step Guide: How Data is Fetched from the API

### 1. Extraction of Country Codes from the Rest Countries API
The goal is to extract current weather information from the OpenWeather API for multiple cities, such as Abuja, London, and Cairo, and load this data into a database. The API request can fetch data for specified cities using parameters like city name, state code, and country code. (Note: Searching by state is only available for U.S. locations.)

**API Calls:**
- `https://api.openweathermap.org/data/2.5/weather?q={city name}&appid={API key}`
- `https://api.openweathermap.org/data/2.5/weather?q={city name},{country code}&appid={API key}`
- `https://api.openweathermap.org/data/2.5/weather?q={city name},{state code},{country code}&appid={API key}`

Since the API doesn’t allow fetching data for multiple cities simultaneously, each city’s data must be fetched one at a time. For example, to extract data for Abuja, London, and Cairo, each city must be paired with its respective country code.

To obtain these country codes, the country names where the cities are located are passed as arguments to the Rest Countries API.

**API Call:**
- `https://restcountries.com/v3.1/name/{country}`

**Example API Response for Nigeria:**
```json
{
  "name": {
    "common": "Nigeria",
    "official": "Federal Republic of Nigeria",
    "nativeName": {
      "eng": {
        "official": "Federal Republic of Nigeria",
        "common": "Nigeria"
      }
    }
  },
  "tld": [".ng"],
  "cca2": "NG"
}
```
The `cca2` key represents the country code, which is extracted for use in the OpenWeather API calls.

### 2. Extraction of Geographical Information from the OpenWeather API
With the country codes obtained, the next step is to fetch the geographical information (latitude and longitude) for each city using the OpenWeather API.

**API Call Example for Abuja (Nigeria):**
- `https://api.openweathermap.org/data/2.5/weather?q=abuja,ng&appid={API key}`

**Example API Response:**
```json
{
  "name": "Abuja",
  "local_names": {"az": "Abuca", "fa": "آبوجا", ...},
  "lat": 9.0643305,
  "lon": 7.4892974,
  "country": "NG",
  "state": "Federal Capital Territory"
}
```
This returns the city’s name, local names, country, latitude, and longitude.

### 3. Extraction of Current Weather Data from the OpenWeather API
The main data to be extracted—current weather conditions—is fetched using the latitude and longitude obtained in the previous step.

**API Call:**
- `https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={part}&appid={API key}`

**Parameters:**
- `lat`: Latitude, decimal (-90; 90).
- `lon`: Longitude, decimal (-180; 180).
- `appid`: The unique API key.
- `exclude`: Optional. Excludes certain parts of the weather data (e.g., minutely, hourly, daily, alerts).

An Airflow environment variable `WEATHER_FIELDS_EXCLUDE` is used to exclude parts not needed (e.g., `minutely,hourly,daily,alerts`), retaining only `current` data.

**Example API Call for Abuja:**
- `https://api.openweathermap.org/data/3.0/onecall?lat=9.07&lon=7.49&exclude=hourly,daily,alerts,minutely&appid={API key}`

**Example API Response:**
```json
{
  "lat": 9.07,
  "lon": 7.49,
  "timezone": "Africa/Lagos",
  "timezone_offset": 3600,
  "current": {
    "dt": 1726854764,
    "sunrise": 1726809568,
    "sunset": 1726853243,
    "temp": 297.5,
    ...
  }
}
```
This returns fields such as current weather data, latitude, longitude, and timezone for the specified city.

### 4. Merging Current Weather Data and Geographical Data
The extracted current weather data is merged with the geographical information to create a comprehensive dataset useful for analytics. 

**Merged Data Example:**
```json
{
  "lat": 6.46,
  "lon": 3.39,
  "timezone": "Africa/Lagos",
  "timezone_offset": 3600,
  "current": { "dt": 1726747705, "sunrise": 1726724175, ... },
  "city": "Lagos",
  "country": "Nigeria",
  "state": "Lagos"
}
```
For each city specified in the Airflow configuration, the data is extracted and merged.

### 5. Transforming the Merged Data
The merged data is transformed into a list of dictionaries, where each dictionary represents the processed weather data for each city. This data is structured to be easily loaded into a PostgreSQL database.

**Example Transformed Record:**
```json
{
  "city": "Lagos",
  "country": "NG",
  "state": "Lagos State",
  "latitude": 6.46,
  "longitude": 3.39,
  "timezone": "Africa/Lagos",
  ...
}
```

### 6. Loading into PostgreSQL Database (Final Step)
The processed weather records are loaded into the database using a delete-write pattern to avoid duplicates. A cloud PostgreSQL database was set up on Render, and the data is inserted for use in analytics.

**Database Diagram:**

![alt text](images\image.png)
