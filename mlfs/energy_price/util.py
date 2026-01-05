import os
import datetime
import time
import requests
import pandas as pd
import json
from geopy.geocoders import Nominatim
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.ticker import MultipleLocator
import openmeteo_requests
import requests_cache
from retry_requests import retry
import hopsworks
import hsfs
from pathlib import Path

def get_historical_weather(city, start_date,  end_date, latitude, longitude):
    # latitude, longitude = get_city_coordinates(city)

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ["temperature_2m_mean", "precipitation_sum", "wind_speed_10m_max", "wind_direction_10m_dominant"]
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(1).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(2).ValuesAsNumpy()
    daily_wind_direction_10m_dominant = daily.Variables(3).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s"),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s"),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
    )}
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
    daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant

    daily_dataframe = pd.DataFrame(data = daily_data)
    daily_dataframe = daily_dataframe.dropna()
    daily_dataframe['city'] = city
    return daily_dataframe

def get_hourly_weather_forecast(city, latitude, longitude):

    # latitude, longitude = get_city_coordinates(city)

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/ecmwf"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ["temperature_2m", "precipitation", "wind_speed_10m", "wind_direction_10m"]
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data. The order of variables needs to be the same as requested.

    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(2).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(3).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s"),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}
    hourly_data["temperature_2m_mean"] = hourly_temperature_2m
    hourly_data["precipitation_sum"] = hourly_precipitation
    hourly_data["wind_speed_10m_max"] = hourly_wind_speed_10m
    hourly_data["wind_direction_10m_dominant"] = hourly_wind_direction_10m

    hourly_dataframe = pd.DataFrame(data = hourly_data)
    hourly_dataframe = hourly_dataframe.dropna()
    return hourly_dataframe



def get_city_coordinates(city_name: str):
    """
    Takes city name and returns its latitude and longitude (rounded to 2 digits after dot).
    """
    # Initialize Nominatim API (for getting lat and long of the city)
    geolocator = Nominatim(user_agent="MyApp")
    city = geolocator.geocode(city_name)

    latitude = round(city.latitude, 2)
    longitude = round(city.longitude, 2)

    return latitude, longitude

def trigger_request(url:str):
    response = requests.get(url)
    if response.status_code == 200:
        # Extract the JSON content from the response
        data = response.json()
    else:
        print("Failed to retrieve data. Status Code:", response.status_code)
        raise requests.exceptions.RequestException(response.status_code)

    return data




def get_energy_price(date=None):
    #we do not have access to the energy price API, so we will manually update this value for now
    energy_prices = {
        "2026-01-06": 1177.50,
        "2026-01-05": 1501.55,
        "2026-01-04": 1005.08,
        "2026-01-03": 675.03,
        "2026-01-02": 248.68,
        "2026-01-01": 144.87,
        "2025-12-31": 683.73,
        "2025-12-30": 343.22,
        "2025-12-29": 29.34,
        "2025-12-28": 25.80,
        "2025-12-27": 13.50,
        "2025-12-26": 25.06,
        "2025-12-25": 13.09,
        "2025-12-24": 55.76,
    }
    if date is None:
        return energy_prices
    
    date_str = date.strftime("%Y-%m-%d")
    if date_str in energ_prices:
        return energy_prices[date_str]
    else:
        print(f"Error: Energy price for {date} is not available.")
        raise ValueError(f"Energy price for {date} is not available.")


import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import MultipleLocator

def plot_energy_forecast(df: pd.DataFrame, file_path: str, hindcast=False):
    fig, ax = plt.subplots(figsize=(10, 6))

    # Convert date column
    day = pd.to_datetime(df['date']).dt.date

    # Plot predicted price
    ax.plot(day, df['predicted_sek'], label='Predicted Price (SEK)', color='red', linewidth=2, marker='o', markersize=5, markerfacecolor='blue')

    # Axes
    ax.set_xlabel('Date')
    ax.set_ylabel('Price (SEK)')
    ax.set_title(f"Energy Price Forecast (SEK)")
    ax.grid(True, which='major', axis='y', linestyle='--', alpha=0.7)
    if len(df.index) > 11:
        every_x_tick = int(len(df.index) / 10)
        ax.xaxis.set_major_locator(MultipleLocator(every_x_tick))
    
    plt.xticks(rotation=45)

    # If hindcast
    if hindcast:
        if 'sek' in df.columns:ax.plot(day, df['sek'], label='Actual Price (SEK)', color='black', linewidth=2, marker='^', markersize=5, markerfacecolor='grey')

    # Layout
    ax.legend(loc='best')
    plt.tight_layout()

    # Save and return
    plt.savefig(file_path)
    return plt


def delete_feature_groups(fs, name):
    try:
        for fg in fs.get_feature_groups(name):
            fg.delete()
            print(f"Deleted {fg.name}/{fg.version}")
    except hsfs.client.exceptions.RestAPIError:
        print(f"No {name} feature group found")

def delete_feature_views(fs, name):
    try:
        for fv in fs.get_feature_views(name):
            fv.delete()
            print(f"Deleted {fv.name}/{fv.version}")
    except hsfs.client.exceptions.RestAPIError:
        print(f"No {name} feature view found")

def delete_models(mr, name):
    models = mr.get_models(name)
    if not models:
        print(f"No {name} model found")
    for model in models:
        model.delete()
        print(f"Deleted model {model.name}/{model.version}")

def delete_secrets(proj, name):
    secrets = secrets_api(proj.name)
    try:
        secret = secrets.get_secret(name)
        secret.delete()
        print(f"Deleted secret {name}")
    except hopsworks.client.exceptions.RestAPIError:
        print(f"No {name} secret found")

# WARNING - this will wipe out all your feature data and models
def purge_project(proj):
    fs = proj.get_feature_store()
    mr = proj.get_model_registry()

    # Delete Feature Views before deleting the feature groups
    delete_feature_views(fs, "air_quality_fv")

    # Delete ALL Feature Groups
    delete_feature_groups(fs, "air_quality")
    delete_feature_groups(fs, "weather")
    delete_feature_groups(fs, "aq_predictions")

    # Delete all Models
    delete_models(mr, "air_quality_xgboost_model")
    delete_secrets(proj, "SENSOR_LOCATION_JSON")

def check_file_path(file_path):
    my_file = Path(file_path)
    if my_file.is_file() == False:
        print(f"Error. File not found at the path: {file_path} ")
    else:
        print(f"File successfully found at the path: {file_path}")

"""def backfill_predictions_for_monitoring(weather_fg, air_quality_df, monitor_fg, model):
    features_df = weather_fg.read()
    features_df = features_df.sort_values(by=['date'], ascending=True)
    features_df = features_df.tail(10)
    features_df['predicted_pm25'] = model.predict(features_df[['temperature_2m_mean', 'precipitation_sum', 'wind_speed_10m_max', 'wind_direction_10m_dominant']])
    df = pd.merge(features_df, air_quality_df[['date','pm25','street','country']], on="date")
    df['days_before_forecast_day'] = 1
    hindcast_df = df
    df = df.drop('pm25', axis=1)
    monitor_fg.insert(df, write_options={"wait_for_job": True})
    return hindcast_df"""

def backfill_predictions_for_monitoring(weather_fg, energy_price_df, monitor_fg, model):
    features_df = weather_fg.read()
    features_df['date'] = pd.to_datetime(features_df['date']).dt.tz_localize(None)
    energy_price_df['date'] = pd.to_datetime(energy_price_df['date']).dt.tz_localize(None)
    
    df = pd.merge(features_df, energy_price_df[['date', 'sek']], on="date", how="inner")
    df = df.sort_values(by=['date'], ascending=True).tail(20)
    
    feature_cols = [
        'temperature_2m_mean_flasjon', 'precipitation_sum_flasjon',
        'wind_speed_10m_max_flasjon', 'wind_direction_10m_dominant_flasjon',
        'temperature_2m_mean_hudiksvall', 'precipitation_sum_hudiksvall',
        'wind_speed_10m_max_hudiksvall', 'wind_direction_10m_dominant_hudiksvall',
        'temperature_2m_mean_ange', 'precipitation_sum_ange',
        'wind_speed_10m_max_ange', 'wind_direction_10m_dominant_ange',
        'temperature_2m_mean_solleftea', 'precipitation_sum_solleftea',
        'wind_speed_10m_max_solleftea', 'wind_direction_10m_dominant_solleftea',
        'temperature_2m_mean_umea', 'precipitation_sum_umea',
        'wind_speed_10m_max_umea', 'wind_direction_10m_dominant_umea',
    ]

    df['predicted_sek'] = model.predict(df[feature_cols])
    df['days_before_forecast_day'] = 1
    
    insert_df = df.drop(columns=['sek'])
    monitor_fg.insert(insert_df, write_options={"wait_for_job": True})
    
    hindcast_df = df[['date', 'predicted_sek', 'sek']]
    return hindcast_df
    


