
#import librairies
import requests
import pandas as pd
from datetime import datetime, timedelta
from retry_requests import retry
import openmeteo_requests
import requests_cache
from geopy.geocoders import Nominatim
import sys
print(f"Module exécuté dans : {sys.executable}")
#import my own functions
from DF_functions import *





# -----------------------SETUP OPEN-METEO API------------------------#
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)
# -----------------------SETUP GEOPY API----------------------------#
geolocator = Nominatim(user_agent='myapplication')




# -------------------- GEOLOCALISATION --------------------#

def get_geolocation(city):
    """
    Retrieves the latitude and longitude of a given city.

    Args:
        city (str): The name of the city.

    Returns:
        dict: A dictionary containing 'latitude' and 'longitude'.
    """

    location = geolocator.geocode(city)
    return {'latitude': location.raw['lat'], 'longitude': location.raw['lon']}


def get_geolocation_openmeteo(city, language='en',nb_results=1):

    """
    Retrieves informations about a given city.

    Args:
        city (str): The name of the city.

    Returns:

    dict: A dictionary containing    latitutde, longitude, elevation, population, postcodes, admin1, admin2, admin3, admin4
                                      (which are string	name of hierarchical administrative areas this location resides in. 
                                    Admin1 is the first administrative level. Admin2 the second administrative level, etc.)
                                    , admin1_id,admin2_id, admin3_id, admin4_id.
    """
    base_url = 'https://geocoding-api.open-meteo.com/v1'
    endpoint = "search"
    params = {'name': city
            ,'count': nb_results
            ,'language':language
            ,'format':'json'}
    headers = {}
    json_data = get_data(base_url, endpoint, params=params, headers=headers)
    try :
        if nb_results ==1:
            location = json_data[0]['results'][0]
        else :
            location = json_data[0]['results']
    except(KeyError,TypeError):
        raise ValueError(f"No results found for city '{city}'.")


    df_geo = build_table(location)
    df_geo["probability"] = range(1, len(df_geo) + 1)
    return df_geo 




#------------------------------WEATHER-------------------------------------#
# --------------------CONSTANTS--------------------#

full_current_variables = ["temperature_2m", "precipitation", "weather_code", 
                          "wind_speed_10m", "relative_humidity_2m", "rain", 
                          "cloud_cover", "wind_direction_10m", "apparent_temperature", 
                          "showers", "pressure_msl", "wind_gusts_10m", "is_day", "snowfall", 
                          "surface_pressure"]


full_forecast_daily_variables =  ["weather_code", "apparent_temperature_min", "sunshine_duration",
                                   "rain_sum", "precipitation_probability_max", "shortwave_radiation_sum", 
                                   "temperature_2m_max", "sunrise", "uv_index_max", "showers_sum", "wind_speed_10m_max",
                                   "et0_fao_evapotranspiration", "temperature_2m_min", "sunset", "uv_index_clear_sky_max", 
                                   "snowfall_sum", "wind_gusts_10m_max", "daylight_duration", "apparent_temperature_max", 
                                   "precipitation_sum", "precipitation_hours", "wind_direction_10m_dominant"]

full_forecast_hourly_variables = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature",
                                   "precipitation_probability", "precipitation", "rain", "showers", "snowfall", 
                                   "snow_depth", "weather_code", "pressure_msl", "surface_pressure", "cloud_cover",
                                    "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "visibility", "evapotranspiration", 
                                    "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_120m",
                                    "wind_speed_80m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", 
                                    "wind_direction_120m", "wind_direction_180m", "temperature_80m", "wind_gusts_10m", 
                                    "temperature_120m", "temperature_180m", "soil_temperature_0cm", "soil_temperature_6cm",
                                    "soil_temperature_18cm", "soil_temperature_54cm", "soil_moisture_0_to_1cm", 
                                    "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm",
                                    "soil_moisture_27_to_81cm"]

full_historical_daily_variables =  ["weather_code", "apparent_temperature_min", "sunshine_duration", 
                                    "rain_sum", "precipitation_probability_max", "shortwave_radiation_sum",
                                    "temperature_2m_max", "sunrise", "uv_index_max", "showers_sum", 
                                    "wind_speed_10m_max", "et0_fao_evapotranspiration", "temperature_2m_min", 
                                    "sunset", "uv_index_clear_sky_max", "snowfall_sum", "wind_gusts_10m_max",
                                    "daylight_duration", "apparent_temperature_max", "precipitation_sum", "precipitation_hours",
                                    "wind_direction_10m_dominant"]

#--------------------------current weather ---------------------------------
def get_current_weather(city,weather_variables='no_variables', nb_results=1, language='en'):
    """""
    
    Retrieves current weather data for a given city.

    Args :
        city (str): The name of the city.
        forecast_variables (list or str, optional): List of variables to retrieve or 'no_variables' for default.
        forecast_days (int, optional): Number of forecast days (default is 1). --> TO DELETE FROM THE FUNCTION

    Returns :
        pd.DataFrame: A DataFrame containing the current weather data.

    
    Weather_variables : ["temperature_2m", "precipitation", "weather_code", "wind_speed_10m", "relative_humidity_2m", "rain", "cloud_cover", "wind_direction_10m", "apparent_temperature", "showers", "pressure_msl", "wind_gusts_10m", "is_day", "snowfall", "surface_pressure"]
    """

    if weather_variables == 'no_variables':
        weather_variables = ["temperature_2m", "precipitation", "weather_code", "wind_speed_10m", "relative_humidity_2m", "rain", "cloud_cover", "wind_direction_10m", "apparent_temperature", "showers", "pressure_msl", "wind_gusts_10m", "is_day", "snowfall", "surface_pressure"]



    base_url ='https://api.open-meteo.com/v1/forecast'
    #old way to extract the lon and lat 
    #get_geolocation(city)
    location = get_geolocation_openmeteo(city, nb_results=nb_results, language=language)
    lon = location.loc[location['probability']==1, 'longitude'].unique()[0]
    lat = location.loc[location['probability']==1, 'latitude'].unique()[0]
    #lon = lon[0]
    #lat = lat[0]
    params = {
	"latitude": lat,
	"longitude": lon,
	"current": weather_variables,
	#"forecast_days": forecast_days
    }
    forecast_response = openmeteo.weather_api(base_url, params=params)
    response = forecast_response[0]
    current_weather = response.Current()
    dict_current_weather = {}
    count = 0
    for var in weather_variables:
        dict_current_weather[var]= current_weather.Variables(count).Value()
        count+=1
    
    df_current = build_table(dict_current_weather)
    df_current['latitude'] = lat
    df_current['longitude'] = lon
    df_current['City'] = city
    df_current['Time'] = datetime.utcnow().time().strftime('%H:%M')
    df_current['Date'] = datetime.utcnow().date()
    # Reversing the column order
    cols = list(df_current.columns)
    cols.reverse()
    df_current = df_current[cols]
    return df_current



#FUNCTION TO GET FORECAST DAILY WEATHER

def get_forecast_daily_weather(city, weather_variables='no_variables', forecast_days=3,  nb_results=1, language='en'):
    """
    Retrieves daily weather forecast data for a given city.

    Args:
        city (str): The name of the city.
        forecast_variables (list or str, optional): List of variables to retrieve or 'no_variables' for default.
        forecast_days (int, optional): Number of forecast days (default is 3).

    Returns:
        pd.DataFrame: A DataFrame containing the daily weather forecast.

    
    Weather_variables = ["apparent_temperature_min", "temperature_2m_max", "sunrise", "sunshine_duration",  "rain_sum", "precipitation_probability_max", "uv_index_max", "wind_speed_10m_max",  "temperature_2m_min", "sunset", "snowfall_sum", "uv_index_clear_sky_max", "apparent_temperature_max",  "daylight_duration", "precipitation_sum", "precipitation_hours"]
    """

    if weather_variables == 'no_variables':
        weather_variables = [
            "weather_code", "apparent_temperature_min", "sunshine_duration", "rain_sum",
            "precipitation_probability_max", "shortwave_radiation_sum", "temperature_2m_max",
            "sunrise", "uv_index_max", "showers_sum", "wind_speed_10m_max",
            "et0_fao_evapotranspiration", "temperature_2m_min", "sunset", "uv_index_clear_sky_max",
            "snowfall_sum", "wind_gusts_10m_max", "daylight_duration", "apparent_temperature_max",
            "precipitation_sum", "precipitation_hours", "wind_direction_10m_dominant"
        ]

    base_url = 'https://api.open-meteo.com/v1/forecast'
     #old way to extract the lon and lat 
    #location = get_geolocation(city)
    #lat, lon = location['latitude'], location['longitude']

    location = get_geolocation_openmeteo(city, nb_results=nb_results, language=language)
    lon = location.loc[location['probability']==1, 'longitude'].unique()[0]
    lat = location.loc[location['probability']==1, 'latitude'].unique()[0] 
    #lon = lon[0]
    #lat = lat[0]

    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": weather_variables,
        "forecast_days": forecast_days
    }

    forecast_response = openmeteo.weather_api(base_url, params=params)
    response = forecast_response[0]
    daily_weather = response.Daily()
    dict_forecast_weather = {}
    count = 0



    daily_data = {"forecast_day": pd.date_range(
    start = pd.to_datetime(daily_weather.Time(), unit = "s", utc = True),
    end = pd.to_datetime(daily_weather.TimeEnd(), unit = "s", utc = True),
    freq = pd.Timedelta(seconds = daily_weather.Interval()),
    inclusive = "left"
    )}



    for var in weather_variables:
        dict_forecast_weather[var]= daily_weather.Variables(count).ValuesAsNumpy()
        count+=1
        daily_data[var] = dict_forecast_weather[var]

    forecast_daily_dataframe = pd.DataFrame(data = daily_data)
    forecast_daily_dataframe['latitude'] = lat
    forecast_daily_dataframe['longitude'] = lon
    cols = list(forecast_daily_dataframe.columns)
    cols.reverse()
    forecast_daily_dataframe = forecast_daily_dataframe[cols]
    #adding context to the result
    forecast_daily_dataframe['City'] = city
    forecast_daily_dataframe['Requested_Date'] = datetime.utcnow().date()
    # Reversing the column order
    cols = list(forecast_daily_dataframe.columns)
    cols.reverse()
    forecast_daily_dataframe = forecast_daily_dataframe[cols]
    return forecast_daily_dataframe



#FUNCTION TO FORECAST HOURLY WEATHER

def get_forecast_hourly_weather(city, weather_variables='no_variables', forecast_days=3,nb_results=1, language='en'):
    """
    Retrieves hourly weather forecast data for a given city.

    Args:
        city (str): The name of the city.
        forecast_h_variables (list or str, optional): List of variables to retrieve or 'no_variables' for default.
        forecast_days (int, optional): Number of forecast days (default is 3).

    Returns:
        pd.DataFrame: A DataFrame containing the hourly weather forecast.

    Weather_variables = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation_probability", 
                            "precipitation", "rain", "showers", "snowfall", "snow_depth", "weather_code", "pressure_msl", "surface_pressure",
                            "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "visibility", "evapotranspiration", 
                            "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_120m", "wind_speed_80m",
                            "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m",
                            "temperature_80m", "wind_gusts_10m", "temperature_120m", "temperature_180m", "soil_temperature_0cm", "soil_temperature_6cm",
                             "soil_temperature_18cm", "soil_temperature_54cm", "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm",
                             "soil_moisture_9_to_27cm", "soil_moisture_27_to_81cm"]
    """

    if weather_variables =='no_variables':
        weather_variables = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation_probability", "precipitation", "rain", "showers", "snowfall", "snow_depth", "weather_code", "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "visibility", "evapotranspiration", "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_120m", "wind_speed_80m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m", "temperature_80m", "wind_gusts_10m", "temperature_120m", "temperature_180m", "soil_temperature_0cm", "soil_temperature_6cm", "soil_temperature_18cm", "soil_temperature_54cm", "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm", "soil_moisture_27_to_81cm"]


    base_url ='https://api.open-meteo.com/v1/forecast'
     #old way to extract the lon and lat 
    #get_geolocation(city)
    #lat = get_geolocation(city)['latitude']
    #lon = get_geolocation(city)['longitude']
    location = get_geolocation_openmeteo(city, nb_results=nb_results, language=language)
    lon = location.loc[location['probability']==1, 'longitude'].unique()[0]
    lat = location.loc[location['probability']==1, 'latitude'].unique()[0] 
   #lon = lon[0]
    #lat = lat[0]


    params = {
	"latitude": lat,
	"longitude": lon,
	"hourly": weather_variables,
	"forecast_days": forecast_days
    }
    response = openmeteo.weather_api(base_url, params=params)
    response = response[0]
    hourly_forecast = response.Hourly()
    hourly_dict = {"date": pd.date_range(
	start = pd.to_datetime(hourly_forecast.Time(), unit = "s", utc = True),
	end = pd.to_datetime(hourly_forecast.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly_forecast.Interval()),
	inclusive = "left"
    )}

    dict_var = {}
    count = 0
    for var in weather_variables:
        dict_var[var]= hourly_forecast.Variables(count).ValuesAsNumpy()
        count+=1
        hourly_dict[var] = dict_var[var]

    foreast_hourly_dataframe = pd.DataFrame(data = hourly_dict)
    foreast_hourly_dataframe['latitude'] = lat
    foreast_hourly_dataframe['longitude'] = lon


    #adding context to the result
    foreast_hourly_dataframe['Forecast_Hour'] = foreast_hourly_dataframe['date'].dt.strftime('%H:%M')
    foreast_hourly_dataframe['Forecast_Date'] = foreast_hourly_dataframe['date'].dt.date
    foreast_hourly_dataframe['City'] = city
    foreast_hourly_dataframe['Requested_Date'] = datetime.utcnow().date()
    # Reversing the column order
    cols = list(foreast_hourly_dataframe.columns)
    cols.reverse()
    foreast_hourly_dataframe = foreast_hourly_dataframe[cols]
    foreast_hourly_dataframe = foreast_hourly_dataframe.drop(columns=['date'])
    #return dict_forecast_weather
    return foreast_hourly_dataframe



#FUNCTION TO GET HISTORICAL DAILY WEATHER 

def get_daily_historical_weather(city, start_date,end_date,weather_variables='no_variables',nb_results=1, language='en'):


    """""

 
    Retrieves historical daily weather data for a given city.

    Args:
        city (str): The name of the city.
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
        weather_variables (list or str, optional): List of variables to retrieve or 'no_variables' for default.

    Returns:
        pd.DataFrame: A DataFrame containing the hourly weather forecast.
    
    
    Weather variables : ["apparent_temperature_min", "temperature_2m_max", "sunrise", "sunshine_duration", 
                         "rain_sum", "precipitation_probability_max", "uv_index_max", "wind_speed_10m_max", 
                         "temperature_2m_min", "sunset", "snowfall_sum", "uv_index_clear_sky_max", "apparent_temperature_max",
                         "daylight_duration", "precipitation_sum", "precipitation_hours"]
    
    """""

    if weather_variables=='no_variables':
        weather_variables = ["weather_code", "apparent_temperature_min", "sunshine_duration", "rain_sum", "precipitation_probability_max", "shortwave_radiation_sum", "temperature_2m_max", "sunrise", "uv_index_max", "showers_sum", "wind_speed_10m_max", "et0_fao_evapotranspiration", "temperature_2m_min", "sunset", "uv_index_clear_sky_max", "snowfall_sum", "wind_gusts_10m_max", "daylight_duration", "apparent_temperature_max", "precipitation_sum", "precipitation_hours", "wind_direction_10m_dominant"]
        

    base_url ="https://archive-api.open-meteo.com/v1/archive"
     #old way to extract the lon and lat 
    #get_geolocation(city)
    #lat = get_geolocation(city)['latitude']
    #lon = get_geolocation(city)['longitude']
    location = get_geolocation_openmeteo(city, nb_results=nb_results, language=language)
    lon = location.loc[location['probability']==1, 'longitude'].unique()[0]
    lat = location.loc[location['probability']==1, 'latitude'].unique()[0] 
    #lon = lon[0]
    #lat = lat[0]

    params = {
	"latitude": lat,
	"longitude": lon,
    "start_date":start_date,
    "end_date":end_date,
	"daily": weather_variables,
    #"timezone": timezone,
    }
    historical_response = openmeteo.weather_api(base_url, params=params)
    response = historical_response[0]
    h_daily_weather = response.Daily()
 
    daily_data = {"date": pd.date_range(
	start = pd.to_datetime(h_daily_weather.Time(), unit = "s", utc = True),
	end = pd.to_datetime(h_daily_weather.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = h_daily_weather.Interval()),
	inclusive = "left"
    )}

    dict_historical_weather = {}
    count = 0
    for var in weather_variables:
        dict_historical_weather[var]= h_daily_weather.Variables(count).ValuesAsNumpy()
        count+=1
        daily_data[var] = dict_historical_weather[var]

    historical_daily_dataframe = pd.DataFrame(data = daily_data)
    historical_daily_dataframe['latitude'] = lat
    historical_daily_dataframe['longitude'] = lon
    #adding context to the result

    historical_daily_dataframe['Historical_Day'] = historical_daily_dataframe['date'].dt.strftime('%a')
    historical_daily_dataframe['Historical_Month'] = historical_daily_dataframe['date'].dt.strftime('%m')
    historical_daily_dataframe['Historical_Year'] = historical_daily_dataframe['date'].dt.strftime('%Y')
    historical_daily_dataframe['Historical_Date'] = historical_daily_dataframe['date'].dt.date
    historical_daily_dataframe['City'] = city

    # Reversing the column order
    cols = list(historical_daily_dataframe.columns)
    cols.reverse()
    historical_daily_dataframe = historical_daily_dataframe[cols]
    historical_daily_dataframe = historical_daily_dataframe.drop(columns=['date'])
    #return dict_forecast_weather
    return historical_daily_dataframe

#FUNCTION TO FORECAST FOR HOUR WEATHER
#it is possible to get also the infos to get hourly the historcial weather but not pertinent yet, so I did not extract it.
