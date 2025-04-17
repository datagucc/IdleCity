#Import libraries
import sys
import os
import time

my_current_loc = os.getcwd()
    # Ajouter le dossier parent d'Entrega_Final au chemin d'importation
current_loc = os.path.dirname(os.path.abspath(__file__))  # obtenir le chemin absolu du script actuel
parent_dir = os.path.dirname(current_loc)  # obtenir le dossier parent (Entrega_Final)

    # Ajouter le dossier Modules au sys.path pour l'importation
sys.path.append(os.path.join(parent_dir, 'Modules'))

    # Maintenant vous pouvez importer vos modules
from DF_functions import *
from openmeteo_API import *

os.chdir(my_current_loc)
#os.chdir('../')

def main():
    #--------------------1. GEOLOCALISATION ------------------------
    # Load DeltaTable from the bronze layer.
    name_folder = 'Data/Bronze/OpenMeteo/Others/Geolocation'
    my_dt = DeltaTable(name_folder).to_pandas()

    #drop  columns
    my_dt = my_dt.drop(columns=['admin4', 'admin4_id','__index_level_0__'], axis=1)

    #rename columns
    my_dt = my_dt.rename(columns={"elevation":"elevation_m"})

    #replace missing values
    imputation_mapping = {
                  'population' : -1
              ,'admin1_id':-1
                      ,'admin2_id' : -1
              ,'admin3_id' : -1
              ,'admin1':'no_value'
              ,'admin2' : 'no_value'
              ,'admin3' : 'no_value'
            }
    my_dt = my_dt.fillna(imputation_mapping)

    #change types
    conversion_mapping = {
        "admin1_id": "int64",
        "admin2_id": "int64",
        "admin3_id": "int64",
        "admin1_id": "int64",
        "population": "int64",
        "name": "string",
        "feature_code": "string",
        "country_code": "string",
        "timezone": "string",
        "country": "string",
        "admin1": "string",
        "admin2": "string",
        "admin3": "string",
        "probability": "int8",
        }
    my_dt = my_dt.astype(conversion_mapping)

    #order table
    my_dt = order_table(my_dt, ['country', 'name'])


    #STORING THE DATA
    name_folder = 'Data/Silver/OpenMeteo/Others/Geolocation'
    partition_cols = None
    predicate = "target.id = source.id"
    save_new_data_as_delta(my_dt,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Silver', source= 'open-meteo-geoloc', author ='Augustin')



    #-------------------------------------------------------------------


    #--------------------------2. WEATHERCODE -------------------------

    name_folder = 'Data/Bronze/OpenMeteo/Others/WeatherCode'
    my_dt = DeltaTable(name_folder).to_pandas()

    #rename columns
    my_dt = my_dt.rename(columns={"Code": "weather_code"})
    my_dt = my_dt.rename(columns={"Description": "description"})

    # Split comma-separated values into lists and explode into rows
    my_dt['weather_code'] = my_dt['weather_code'].astype(str).str.split(",")
    my_dt['description'] = my_dt['description'].astype(str).str.split(",")
    my_dt = my_dt.explode(['weather_code', 'description'], ignore_index=True)

    # Clean and format Description column
    my_dt['description'] = my_dt['description'].str.strip().str.capitalize()
    my_dt['weather_code'] = my_dt['weather_code'].str.strip()

    # Order table 
    my_dt = order_table(my_dt, ['weather_code'])


    #STORING THE DATA
    name_folder = 'Data/Silver/OpenMeteo/Others/WeatherCode'
    mode = 'overwrite'
    partition_cols = None
    save_data_as_delta(my_dt,name_folder,mode=mode, partition_cols=partition_cols, layer = 'Silver', source = 'open-meteo-weathercode',author='Augustin')

    #-------------------------------------------------------------------


    #--------------------------3. CURRENT WEATHER -------------------------

    # Load DeltaTable 
    name_folder = 'Data/Bronze/OpenMeteo/Current'
    my_dt = DeltaTable(name_folder).to_pandas()

    # Drop unnecessary column
    my_dt.drop(columns=['__index_level_0__'], inplace=True)

    # Rename columns to include units of measure
    remapping = {
        "cloud_cover": "cloud_cover_inPercent", 
        "relative_humidity_2m": "relative_humidity_2m_inPercent", 
        "apparent_temperature": "apparent_temperature_C", 
        "precipitation_sum": "precipitation_sum_mm",
        "windspeed_10m": "windspeed_10m_kmh",
        "windgusts_10m": "windgusts_10m_kmh", 
        "temperature_2m": "temperature_2m_C",
        "snowfall": "snowfall_cm",
        "surface_pressure": "surface_pressure_hPa", 
        "shortwave_radiation_sum": "shortwave_radiation_sum_Wm2", 
        "precipitation": "precipitation_mm",
        "pressure_msl": "pressure_msl_hPa", 
        "showers": "shower_sum_mm",
        "rain": "rain_mm",
        "wind_speed_10m": "wind_speed_10m_kmh",
        "Date": "date",
        "Time": "time",
        "City": "city"
    }
    my_dt = my_dt.rename(columns=remapping)

    # Change column types
    my_dt["is_day"] = my_dt["is_day"].astype(bool)
    my_dt["weather_code"] = my_dt["weather_code"].astype(int)
    my_dt["weather_code"] = my_dt["weather_code"].astype(str)
    my_dt['weather_code'] = my_dt['weather_code'].str.strip()
    my_dt['longitude'] = my_dt['longitude'].astype(float)
    my_dt['latitude'] = my_dt['latitude'].astype(float)
    my_dt['date'] = pd.to_datetime(my_dt['date'], format='%Y-%m-%d')

    #Ordering the table
    my_dt = order_table(my_dt, ['city','date','time'])

    #STORING THE DATA

    name_folder = 'Data/Silver/OpenMeteo/Current'
    predicate = "target.Date = source.Date AND target.Time = source.Time AND target.City = source.City"
    partition_cols = "Date"
    save_new_data_as_delta(my_dt,name_folder,predicate = predicate, partition_cols=partition_cols, layer = 'Silver', source = 'open-meteo-current',author='Augustin')



    #-------------------------------------------------------------------


    #--------------------------4. FORECAST DAILY -------------------------

    # Load DeltaTable 
    name_folder = 'Data/Bronze/OpenMeteo/Forecast/Daily'
    my_dt = DeltaTable(name_folder).to_pandas()


    # Convert date columns to datetime format
    my_dt['Requested_Date'] = pd.to_datetime(my_dt['Requested_Date'], format='%Y-%m-%d')
    my_dt['forecast_day'] = pd.to_datetime(my_dt['forecast_day'], format='%Y-%m-%d')
    # Change column types
    my_dt['longitude'] = my_dt['longitude'].astype(float)
    my_dt['latitude'] = my_dt['latitude'].astype(float)
    my_dt["weather_code"] = my_dt["weather_code"].astype(int)
    my_dt["weather_code"] = my_dt["weather_code"].astype(str)
    my_dt['weather_code'] = my_dt['weather_code'].str.strip()

    # Rename columns to include units of measure
    remapping = {
        "temperature_2m_max": "temperature_2m_max_C",
        "temperature_2m_min": "temperature_2m_min_C",
        "apparent_temperature_max": "apparent_temperature_max_C",
        "apparent_temperature_min": "apparent_temperature_min_C",
        "precipitation_sum_mm": "precipitation_sum_mm",
        "rain_sum": "rain_sum_mm",
        "showers_sum": "showers_sum_mm",
        "snowfall_sum": "snowfall_sum_cm",
        "precipitation_hours": "precipitation_hours_h",
        "precipitation_probability_max": "precipitation_probability_max_inPercent",
        "sunshine_duration": "sunshine_duration_seconds",
        "daylight_duration": "daylight_duration_seconds",
        "wind_speed_10m_max": "wind_speed_10m_max_kmh",
        "wind_gusts_10m_max": "wind_gusts_10m_max_kmh",
        "wind_direction_10m_dominant": "wind_direction_10m_dominant_deg",
        "shortwave_radiation_sum": "shortwave_radiation_sum_MJm2",
        "et0_fao_evapotranspiration": "et0_fao_evapotranspiration_mm",
        "uv_index_max": "uv_index_max_index",
        "uv_index_clear_sky_max": "uv_index_clear_sky_max_index",
        "Requested_Date": 'requested_date',
        'City': 'city'
    }
    my_dt = my_dt.rename(columns=remapping)

    # Drop irrelevant columns
    my_dt.drop(columns=['sunrise', 'sunset','__index_level_0__'], inplace=True)

    # Convert duration columns to minutes and hours
    my_dt['sunshine_duration_minutes'] = my_dt['sunshine_duration_seconds'] / 60
    my_dt['sunshine_duration_hours'] = my_dt['sunshine_duration_seconds'] / 3600
    my_dt['daylight_duration_minutes'] = my_dt['daylight_duration_seconds'] / 60
    my_dt['daylight_duration_hours'] = my_dt['daylight_duration_seconds'] / 3600


    #ordering the table
    my_dt = order_table(my_dt, ['requested_date','city','forecast_day'])

    #STORING THE DATA
    name_folder = 'Data/Silver/OpenMeteo/Forecast/Daily'
    predicate = """target.requested_date = source.requested_date AND target.city = source.city and target.forecast_day = source.forecast_day """
    partition_cols = ["requested_date"]

    save_new_data_as_delta(my_dt,name_folder,predicate= predicate, partition_cols=partition_cols,layer = 'Silver', source= 'open-meteo-forecast-daily', author ='Augustin')


    #-------------------------------------------------------------------


    #--------------------------5. FORECAST HOURLY -------------------------

    # Load DeltaTable 
    name_folder = 'Data/Bronze/OpenMeteo/Forecast/Hourly'
    my_dt = DeltaTable(name_folder).to_pandas()

    # Drop unnecessary column
    my_dt.drop(columns=['__index_level_0__'], inplace=True)

    # Convert date columns to datetime format
    my_dt['Requested_Date'] = pd.to_datetime(my_dt['Requested_Date'], format='%Y-%m-%d')
    my_dt['Forecast_Date'] = pd.to_datetime(my_dt['Forecast_Date'], format='%Y-%m-%d')
    # Change column types
    my_dt['longitude'] = my_dt['longitude'].astype(float)
    my_dt['latitude'] = my_dt['latitude'].astype(float)
    my_dt["weather_code"] = my_dt["weather_code"].astype(int)
    my_dt["weather_code"] = my_dt["weather_code"].astype(str)
    my_dt['weather_code'] = my_dt['weather_code'].str.strip()


    # Rename columns to include units of measure
    remapping = {
        "soil_moisture_27_to_81cm": "soil_moisture_27_to_81cm_m3m3",
        "soil_moisture_9_to_27cm": "soil_moisture_9_to_27cm_m3m3",
        "soil_moisture_3_to_9cm": "soil_moisture_3_to_9cm_m3m3",
        "soil_moisture_1_to_3cm": "soil_moisture_1_to_3cm_m3m3",
        "soil_moisture_0_to_1cm": "soil_moisture_0_to_1cm_m3m3",
        "soil_temperature_54cm": "soil_temperature_54cm_C",
        "soil_temperature_18cm": "soil_temperature_18cm_C",
        "soil_temperature_6cm": "soil_temperature_6cm_C",
        "soil_temperature_0cm": "soil_temperature_0cm_C",
        "temperature_180m": "temperature_180m_C",
        "temperature_120m": "temperature_120m_C",
        "temperature_80m": "temperature_80m_C",
        "temperature_2m": "temperature_2m_C",
        "wind_gusts_10m": "wind_gusts_10m_kmh",
        "wind_direction_180m": "wind_direction_180m_deg",
        "wind_direction_120m": "wind_direction_120m_deg",
        "wind_direction_80m": "wind_direction_80m_deg",
        "wind_direction_10m": "wind_direction_10m_deg",
        "wind_speed_180m": "wind_speed_180m_kmh",
        "wind_speed_120m": "wind_speed_120m_kmh",
        "wind_speed_80m": "wind_speed_80m_kmh",
        "wind_speed_10m": "wind_speed_10m_kmh",
        "vapour_pressure_deficit": "vapour_pressure_deficit_kPa",
        "et0_fao_evapotranspiration_mm": "et0_fao_evapotranspiration_mm",
        "evapotranspiration": "evapotranspiration_mm",
        "visibility": "visibility_m",
        "cloud_cover_high": "cloud_cover_high_inPercent",
        "cloud_cover_mid": "cloud_cover_mid_inPercent",
        "cloud_cover_low": "cloud_cover_low_inPercent",
        "cloud_cover": "cloud_cover_inPercent",
        "surface_pressure": "surface_pressure_hPa",
        "pressure_msl": "pressure_msl_hPa",
        "weather_code": "weather_code",
        "snow_depth": "snow_depth_m",
        "snowfall": "snowfall_cm",
        "showers": "showers_mm",
        "rain": "rain_mm",
        "precipitation": "precipitation_mm",
        "precipitation_probability": "precipitation_probability_inPercent",
        "apparent_temperature": "apparent_temperature_C",
        "dew_point_2m": "dew_point_2m_C",
        "relative_humidity_2m": "relative_humidity_2m_inPercent",
        "Requested_Date": 'requested_date',
        'City': 'city',
        'Forecast_Date': 'forecast_date',
        'Forecast_Hour': 'forecast_hour'
    }
    my_dt = my_dt.rename(columns=remapping)


    #Ordering columns
    my_dt = order_table(my_dt, ['requested_date','city','forecast_date','forecast_hour'])

    #delete duplicates
    #my_dt.drop_duplicates(keep='first')


    #STORING THE DATA
    name_folder = 'Data/Silver/OpenMeteo/Forecast/Hourly'
    partition_cols = ["requested_date"]
    predicate = """target.requested_date = source.requested_date  AND target.city = source.city AND target.forecast_date = source.forecast_date and target.forecast_hour = source.forecast_hour """

    upsert_data_as_delta(my_dt,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Silver', source= 'open-meteo-forecast-hourly', author ='Augustin')

    # Verifying the data of the bronze layer
    my_dt = DeltaTable(name_folder).to_pandas()



    #-------------------------------------------------------------------


    #--------------------------6. HISTORICAL DAILY -------------------------

    # Load DeltaTable 
    name_folder = 'Data/Bronze/OpenMeteo/Historical/Daily'
    my_dt = DeltaTable(name_folder).to_pandas()

    # Drop columns
    my_dt.drop(columns=['__index_level_0__', 'sunrise', 'sunset', 'uv_index_clear_sky_max', 
                        'uv_index_max', 'precipitation_probability_max'], inplace=True)

    # Rename columns to include units of measure
    remapping = {
        "temperature_2m_max": "temperature_2m_max_C",
        "temperature_2m_min": "temperature_2m_min_C",
        "apparent_temperature_max": "apparent_temperature_max_C",
        "apparent_temperature_min": "apparent_temperature_min_C",
        "precipitation_sum_mm": "precipitation_sum_mm",
        "rain_sum": "rain_sum_mm",
        "showers_sum": "showers_sum_mm",
        "snowfall_sum": "snowfall_sum_cm",
        "precipitation_hours": "precipitation_hours_h",
        "sunshine_duration": "sunshine_duration_seconds",
        "daylight_duration": "daylight_duration_seconds",
        "wind_speed_10m_max": "wind_speed_10m_max_kmh",
        "wind_gusts_10m_max": "wind_gusts_10m_max_kmh",
        "wind_direction_10m_dominant": "wind_direction_10m_dominant_deg",
        "shortwave_radiation_sum": "shortwave_radiation_sum_MJm2",
        "et0_fao_evapotranspiration": "et0_fao_evapotranspiration_mm",
        "Requested_Date": 'requested_date',
        'City': 'city',
        'Historical_Date': 'historical_date',
        'Historical_Year': 'historical_year',
        'Historical_Month': 'historical_month',
        'Historical_Day': 'historical_day'
    }
    my_dt = my_dt.rename(columns=remapping)

    # Convert date columns and calculate durations in minutes and hours
    my_dt['historical_date'] = pd.to_datetime(my_dt['historical_date'], format='%Y-%m-%d')
    my_dt['sunshine_duration_minutes'] = my_dt['sunshine_duration_seconds'] / 60
    my_dt['sunshine_duration_hours'] = my_dt['sunshine_duration_seconds'] / 3600
    my_dt['daylight_duration_minutes'] = my_dt['daylight_duration_seconds'] / 60
    my_dt['daylight_duration_hours'] = my_dt['daylight_duration_seconds'] / 3600

    # Change column types
    my_dt['longitude'] = my_dt['longitude'].astype(float)
    my_dt['latitude'] = my_dt['latitude'].astype(float)
    my_dt["weather_code"] = my_dt["weather_code"].astype(int)
    my_dt["weather_code"] = my_dt["weather_code"].astype(str)
    my_dt['weather_code'] = my_dt['weather_code'].str.strip()


    #Ordering columns
    my_dt = order_table(my_dt, ['city','historical_year','historical_month','historical_day'])

    #STORING THE DATA
    name_folder = 'Data/Silver/OpenMeteo/Historical/Daily'
    predicate = """target.city = source.city AND target.historical_date = source.historical_date"""
    partition_cols = ["historical_year"]

    save_new_data_as_delta(my_dt,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Silver', source= 'open-meteo-historical-daily', author ='Augustin')




    #-------------------------------------------------------------------


    #--------------------------7. CHECKING THE METADATA -------------------------

    name_folder = 'Data/_meta/metadata_table'
    my_dt = DeltaTable(name_folder).to_pandas()
    #Call to my own function to compact the data
    compact_all_silver_tables(my_dt, layer_filter='Silver')

    #Checking silver layer
    name_folder = 'Data/_meta/metadata_table'
    my_dt = DeltaTable(name_folder).to_pandas()
    my_dt = my_dt[my_dt['layer']=='Silver']


    #Comparating silver and bronze
    name_folder = 'Data/_meta/metadata_table'
    my_dt = DeltaTable(name_folder).to_pandas()
    my_dt = my_dt[(my_dt['layer'] == 'Bronze') | (my_dt['layer'] == 'Silver')]
    row_counts_per_table = pd.DataFrame({
        "layer":my_dt["layer"],
        "table_name": my_dt["table_name"],
        "table_path": my_dt["table_path"],
        "total_rows": my_dt['total_rows'],
        "rows_with_at_least_one_nulls":my_dt['rows_with_nulls'],
        "rows_duplicated":my_dt['rows_duplicated']
    })


    print('script silver executed')
    export_metadata_to_excel(layer='Silver')


if __name__ == "__main__":
    main()
