import requests
import json
import time
from influxdb import InfluxDBClient

# InfluxDB credentials
influx_host = '127.0.0.1'
influx_port = 8086
influx_user = 'owmuser'
influx_password = 'owmpass'
influx_dbname = 'owmdb'

# OpenWeatherMap credentials
owm_api_key = '7a9abf89aa1e29b7ba9a072d2793d6fe'
owm_city_id = '1646678'
owm_url = f'https://api.openweathermap.org/data/2.5/weather?id={owm_city_id}&appid={owm_api_key}&units=metric'

# InfluxDB client
client = InfluxDBClient(host=influx_host, port=influx_port, username=influx_user, password=influx_password, database=influx_dbname)

# Function to collect weather data
def collect_weather_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(f"Collecting weather data for city ID {owm_city_id}...")
        return data
    else:
        print(f"Error collecting weather data for city ID {owm_city_id}. Status code: {response.status_code}")
        return None

# Function to write data to InfluxDB
def write_data_to_influxdb(json_body):
    try:
        client.write_points(json_body)
        print("Data written to InfluxDB")
    except Exception as e:
        print(f"Error writing data to InfluxDB: {str(e)}")

# Main loop
while True:
    # Collect weather data
    data = collect_weather_data(owm_url)
    if data is not None:
        # Create InfluxDB schema
        json_body = [
            {
                "measurement": "weather_data",
                "tags": {
                    "city": data['name']
                },
                "time": int(time.time()*1000),
                "fields": {
                    "temperature": data['main']['temp'], 
                    "pressure": data['main']['pressure'],
                    "humidity": data['main']['humidity'],
                    "wind_speed": data['wind']['speed'],
                    "wind_direction": data['wind']['deg']
                }
            }
        ]
        # Write data to InfluxDB
        write_data_to_influxdb(json_body)
    # Delay API calls
    time.sleep(600)
