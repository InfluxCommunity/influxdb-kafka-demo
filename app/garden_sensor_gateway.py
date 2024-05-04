import time
import json
from kafka import KafkaProducer
import requests

def get_weather(api_key, city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        weather_info = {
            "temperature": data["main"]["temp"],
            "description": data["weather"][0]["description"],
            "humidity": data["main"]["humidity"],
            "wind_speed": data["wind"]["speed"],
            "city": data["name"],
            "country": data["sys"]["country"]
        }
        return weather_info
    else:
        print("Failed to fetch weather data:", response.status_code)
        return None

def main():
    api_key = "5bc266ed5946232b512e614896a75f37"  # Replace "YOUR_API_KEY" with your actual API key
    while True:
        city = "Bari"
        weather = get_weather(api_key, city)
        if weather:
            print("Weather in {}, {}: ".format(weather["city"], weather["country"]))
            print("Temperature: {}°C".format(weather["temperature"]))
            print("Description:", weather["description"])
            print("Humidity:", weather["humidity"], "%")
            print("Wind Speed:", weather["wind_speed"], "m/s")

            json_data = json.dumps(weather)
            # Transmit data to kafka
            producer = KafkaProducer(bootstrap_servers=['kafka:9092'])

            producer.send('garden_sensor_data', bytes(f'{json_data}', 'UTF-8'))
            print(f"Sensor data is sent: {json_data}")

        else:
            print("Failed to fetch weather data.")
        time.sleep(5)

if __name__ == "__main__":
    main()

