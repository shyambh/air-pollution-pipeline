from datetime import datetime, timezone
import os
import json
import requests


def get_current_time():
    """Get current UTC timestamp in Unix format"""
    current_utc_time = datetime.now(timezone.utc)
    return int(current_utc_time.timestamp())


def get_unix_time_from_date(date_string: str) -> int:
    """Get Unix timestamp from given date"""
    date_object = datetime.strptime(date_string, "%m-%d-%Y")
    unix_time = int(date_object.replace(tzinfo=timezone.utc).timestamp())
    return unix_time


def get_city_coordinates(city_name: str):
    """Get city coordinates from the given city name"""
    api = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name}&limit=5&appid={os.getenv('OPEN_WEATHER_API_KEY')}"

    response = requests.get(api, timeout=5_000)

    if response.status_code == 200:
        if len(response.content) > 0:
            coordinates = {
                "lat": response.json()[0]["lat"],
                "lon": response.json()[0]["lon"],
            }
        else:
            raise ValueError(
                f"\nCould not find the coordinates for {city_name}. Please enter a valid city name.\n"
            )
        return coordinates

    return None
