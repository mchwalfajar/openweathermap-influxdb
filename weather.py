from influxdb import InfluxDBClient
import requests
import json
import time

# InfluxDB credential
influx_host = '127.0.0.1'
influx_port = 8086
influx_user = 'owmuser'
influx_password = 'owmpass'
influx_dbname = 'owmdb'

# InfluxDB Connection
client = InfluxDBClient(host=influx_host, port=influx_port, username=influx_user, password=influx_password, database=influx_dbname)

# OpenWeatherMap credential
owm_api_key = '7a9abf89aa1e29b7ba9a072d2793d6fe'
owm_city_id = '1646678'
url = f'https://api.openweathermap.org/data/2.5/weather?id={owm_city_id}&appid={owm_api_key}&units=metric'

while True:
    # Call OpenWeatherMap API
    response = (requests.get(url))
    data = response.json()
    print(f"Collecting weather data for city ID {owm_city_id}...")
   
    # InfluxdB schema
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

    # Write ti InfluxDB
    client.write_points(json_body)

    # Call API every 10 minutes
    time.sleep(600)

