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
    #--------------------------1. Current weather -------------------------

    path1 = 'Data/Silver/OpenMeteo/Current'
    path2 = 'Data/Silver/OpenMeteo/Others/Geolocation'
    path3 = 'Data/Silver/OpenMeteo/Others/WeatherCode'
    type_merge = 'left'
    df1_keys = ['latitude','longitude']
    df2_keys = ['latitude','longitude']
    df2_columns = ['elevation_m','timezone','population','country','latitude','longitude']

    df1a_keys = ['weather_code']
    df2a_keys = ['weather_code']
    df2a_columns = ['weather_code','description']

    #cross joining the current weather data with the geolocation data
    df_current = merging_df(path1,path2,type_merge,df1_keys,df2_keys,df2_columns)
    #cross joining the current weather data with the weather code
    df_current_full = merging_df(df_current,path3,type_merge,df1a_keys,df2a_keys,df2a_columns)
    my_dt = df_current_full

    # Add new columns based on temperature thresholds
    my_dt['warm_enough'] = my_dt['temperature_2m_C'] > 23
    my_dt['too_cold'] = my_dt['temperature_2m_C'] < 10

    #STORING THE DATA
    name_folder = 'Data/Gold/OpenMeteo/Current'
    predicate = "target.Date = source.Date AND target.Time = source.Time AND target.City = source.City"
    partition_cols = "Date"
    save_new_data_as_delta(my_dt,name_folder,predicate = predicate, partition_cols=partition_cols, layer = 'Gold', source = 'open-meteo-current',author='Augustin')



    #-------------------------------------------------------------------


    #--------------------------2. Forecast daily -------------------------

    path1 = 'Data/Silver/OpenMeteo/Forecast/Daily'
    path2 = 'Data/Silver/OpenMeteo/Others/Geolocation'
    path3 = 'Data/Silver/OpenMeteo/Others/WeatherCode'
    type_merge = 'left'
    df1_keys = ['latitude','longitude']
    df2_keys = ['latitude','longitude']
    df2_columns = ['elevation_m','timezone','population','country','latitude','longitude']

    df1a_keys = ['weather_code']
    df2a_keys = ['weather_code']
    df2a_columns = ['weather_code','description']

    #cross joining the forecast daily weather data with the geolocation data 
    df_current = merging_df(path1,path2,type_merge,df1_keys,df2_keys,df2_columns)
    #cross joining the forecast daily weather with the weather code 
    df_current_full = merging_df(df_current,path3,type_merge,df1a_keys,df2a_keys,df2a_columns)

    my_dt = df_current_full

    # Add new columns based on conditions
    my_dt['warm_enough'] = my_dt['temperature_2m_max_C'] > 23
    my_dt['too_cold'] = my_dt['temperature_2m_min_C'] < 10
    my_dt['sufficient_sunshine'] = my_dt['sunshine_duration_hours'] > 6



    #STORING THE DATA
    name_folder = 'Data/Gold/OpenMeteo/Forecast/Daily'
    predicate = """target.requested_date = source.requested_date AND target.city = source.city and target.forecast_day = source.forecast_day """
    partition_cols = ["requested_date"]

    save_new_data_as_delta(my_dt,name_folder,predicate= predicate, partition_cols=partition_cols,layer = 'Gold', source= 'open-meteo-forecast-daily', author ='Augustin')


    #-------------------------------------------------------------------


    #--------------------------3. Forecast hourly weather -------------------------


    path1 = 'Data/Silver/OpenMeteo/Forecast/Hourly'
    path2 = 'Data/Silver/OpenMeteo/Others/Geolocation'
    path3 = 'Data/Silver/OpenMeteo/Others/WeatherCode'
    type_merge = 'left'
    df1_keys = ['latitude','longitude']
    df2_keys = ['latitude','longitude']
    df2_columns = ['elevation_m','timezone','population','country','latitude','longitude']

    df1a_keys = ['weather_code']
    df2a_keys = ['weather_code']
    df2a_columns = ['weather_code','description']

    #cross joining the forecast hourly weather data with the geolocation data 
    df_current = merging_df(path1,path2,type_merge,df1_keys,df2_keys,df2_columns)
    #cross joining the forecast hourly weather with the weather code
    df_current_full = merging_df(df_current,path3,type_merge,df1a_keys,df2a_keys,df2a_columns)
    my_dt = df_current_full

    # Add new columns based on conditions
    my_dt['warm_enough'] = my_dt['temperature_2m_C'] > 23
    my_dt['too_cold'] = my_dt['temperature_2m_C'] < 10
    my_dt['too_much_cloud'] = my_dt['cloud_cover_inPercent'] > 65


    #STORING THE DATA
    name_folder = 'Data/Gold/OpenMeteo/Forecast/Hourly'
    partition_cols = ["requested_date"]
    predicate = """target.requested_date = source.requested_date  AND target.city = source.city AND target.forecast_date = source.forecast_date and target.forecast_hour = source.forecast_hour """

    upsert_data_as_delta(my_dt,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Gold', source= 'open-meteo-forecast-hourly', author ='Augustin')


    #-------------------------------------------------------------------


    #--------------------------4. Historical daily weather -------------------------

    path1 = 'Data/Silver/OpenMeteo/Historical/Daily'
    path2 = 'Data/Silver/OpenMeteo/Others/Geolocation'
    path3 = 'Data/Silver/OpenMeteo/Others/WeatherCode'
    type_merge = 'left'
    df1_keys = ['latitude','longitude']
    df2_keys = ['latitude','longitude']
    df2_columns = ['elevation_m','timezone','population','country','latitude','longitude']

    df1a_keys = ['weather_code']
    df2a_keys = ['weather_code']
    df2a_columns = ['weather_code','description']

    #cross joining the historical daily weather data with the geolocation data
    df_current = merging_df(path1,path2,type_merge,df1_keys,df2_keys,df2_columns)
    #cross joining the historical daily weather data with the weather code
    df_current_full = merging_df(df_current,path3,type_merge,df1a_keys,df2a_keys,df2a_columns)
    my_dt = df_current_full

    # Add new columns based on conditions
    my_dt['warm_enough'] = my_dt['temperature_2m_max_C'] > 23
    my_dt['too_cold'] = my_dt['temperature_2m_min_C'] < 10
    my_dt['sufficient_sunshine'] = my_dt['sunshine_duration_hours'] > 6



    #STORING THE DATA
    name_folder = 'Data/Gold/OpenMeteo/Historical/Daily'
    predicate = """target.city = source.city AND target.historical_date = source.historical_date"""
    partition_cols = ["historical_year"]
    #it works with low amount of data but to check if the size increases.
    save_new_data_as_delta(my_dt,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Gold', source= 'open-meteo-historical-daily', author ='Augustin')



    #-------------------------------------------------------------------



    # ---------- 3.2 Apply Aggregations & Comparative Analysis--------
    #--------------------------3.2.1. Means per city per month -------------------------

    name_folder = 'Data/Gold/OpenMeteo/Historical/Daily'
    my_dt = DeltaTable(name_folder).to_pandas()
    groupby = ['city','historical_year', 'historical_month']
    agg_columns = {'wind_direction_10m_dominant_deg':['min','max','mean']
                   ,'precipitation_hours_h':'mean'
                   ,'precipitation_sum':'mean'
                   ,'apparent_temperature_max_C':'mean'
                   ,'daylight_duration_seconds':'mean'
                   ,'wind_gusts_10m_max_kmh':'mean'
                   ,'snowfall_sum_cm':'mean'
                   , 'temperature_2m_min_C':'mean'
                   ,'et0_fao_evapotranspiration_mm':'mean'
                   ,'wind_speed_10m_max_kmh':'mean'
                   ,'showers_sum_mm':'mean'
                   ,'temperature_2m_max_C':'mean'
                   ,'shortwave_radiation_sum_MJm2':'mean'
                   ,'rain_sum_mm':'mean'
                   ,'sunshine_duration_seconds':'mean'
                   ,'apparent_temperature_min_C':'mean'
                   ,'sunshine_duration_minutes':'mean'
                   ,'sunshine_duration_hours':'mean'
                   ,'daylight_duration_minutes':'mean'
                   ,'daylight_duration_hours':'mean'
                   ,'weather_code':lambda x: x.mode().iloc[0] }

    my_dt =aggregate_dataframe(my_dt, groupby, agg_columns)


    #STORING THE DATA
    name_folder = 'Data/Gold/OpenMeteo/Historical/Calculations/PerCityPerMonth'
    save_data_as_delta(my_dt, name_folder, mode="overwrite", layer='Gold',source='open-meteo-historical-daily', author='Augustin')



    #-------------------------------------------------------------------


    #--------------------------3.2.2. Means per city per year -------------------------

    name_folder = 'Data/Gold/OpenMeteo/Historical/Daily'
    my_dt = DeltaTable(name_folder).to_pandas()
    groupby = ['city','historical_year']
    agg_columns = {'wind_direction_10m_dominant_deg':['min','max','mean']
                   ,'precipitation_hours_h':'mean'
                   ,'precipitation_sum':'mean'
                   ,'apparent_temperature_max_C':'mean'
                   ,'daylight_duration_seconds':'mean'
                   ,'wind_gusts_10m_max_kmh':'mean'
                   ,'snowfall_sum_cm':'mean'
                   , 'temperature_2m_min_C':'mean'
                   ,'et0_fao_evapotranspiration_mm':'mean'
                   ,'wind_speed_10m_max_kmh':'mean'
                   ,'showers_sum_mm':'mean'
                   ,'temperature_2m_max_C':'mean'
                   ,'shortwave_radiation_sum_MJm2':'mean'
                   ,'rain_sum_mm':'mean'
                   ,'sunshine_duration_seconds':'mean'
                   ,'apparent_temperature_min_C':'mean'
                   ,'sunshine_duration_minutes':'mean'
                   ,'sunshine_duration_hours':'mean'
                   ,'daylight_duration_minutes':'mean'
                   ,'daylight_duration_hours':'mean'
                   ,'weather_code':lambda x: x.mode().iloc[0] }

    my_dt =aggregate_dataframe(my_dt, groupby, agg_columns)


    #STORING THE DATA
    name_folder = 'Data/Gold/OpenMeteo/Historical/Calculations/PerCityPerYear'
    save_data_as_delta(my_dt, name_folder, mode="overwrite", layer='Gold',source='open-meteo-historical-daily', author='Augustin')



    #-------------------------------------------------------------------


    #-------------------------- 3.2.3. Means overall countries per month  -------------------------



    name_folder = 'Data/Gold/OpenMeteo/Historical/Daily'
    my_dt = DeltaTable(name_folder).to_pandas()
    groupby = ['historical_year','historical_month']
    agg_columns = {'wind_direction_10m_dominant_deg':['min','max','mean']
                   ,'precipitation_hours_h':'mean'
                   ,'precipitation_sum':'mean'
                   ,'apparent_temperature_max_C':'mean'
                   ,'daylight_duration_seconds':'mean'
                   ,'wind_gusts_10m_max_kmh':'mean'
                   ,'snowfall_sum_cm':'mean'
                   , 'temperature_2m_min_C':'mean'
                   ,'et0_fao_evapotranspiration_mm':'mean'
                   ,'wind_speed_10m_max_kmh':'mean'
                   ,'showers_sum_mm':'mean'
                   ,'temperature_2m_max_C':'mean'
                   ,'shortwave_radiation_sum_MJm2':'mean'
                   ,'rain_sum_mm':'mean'
                   ,'sunshine_duration_seconds':'mean'
                   ,'apparent_temperature_min_C':'mean'
                   ,'sunshine_duration_minutes':'mean'
                   ,'sunshine_duration_hours':'mean'
                   ,'daylight_duration_minutes':'mean'
                   ,'daylight_duration_hours':'mean'
                   ,'weather_code':lambda x: x.mode().iloc[0] }

    my_dt =aggregate_dataframe(my_dt, groupby, agg_columns)


    #STORING THE DATA
    name_folder = 'Data/Gold/OpenMeteo/Historical/Calculations/PerMonth'
    save_data_as_delta(my_dt, name_folder, mode="overwrite", layer='Gold',source='open-meteo-historical-daily', author='Augustin')



    #-------------------------------------------------------------------


    #-------------------------3.2.4. Means overall countries per year-------------------------



    name_folder = 'Data/Gold/OpenMeteo/Historical/Daily'
    my_dt = DeltaTable(name_folder).to_pandas()
    groupby = ['historical_year']
    agg_columns = {'wind_direction_10m_dominant_deg':['min','max','mean']
                   ,'precipitation_hours_h':'mean'
                   ,'precipitation_sum':'mean'
                   ,'apparent_temperature_max_C':'mean'
                   ,'daylight_duration_seconds':'mean'
                   ,'wind_gusts_10m_max_kmh':'mean'
                   ,'snowfall_sum_cm':'mean'
                   , 'temperature_2m_min_C':'mean'
                   ,'et0_fao_evapotranspiration_mm':'mean'
                   ,'wind_speed_10m_max_kmh':'mean'
                   ,'showers_sum_mm':'mean'
                   ,'temperature_2m_max_C':'mean'
                   ,'shortwave_radiation_sum_MJm2':'mean'
                   ,'rain_sum_mm':'mean'
                   ,'sunshine_duration_seconds':'mean'
                   ,'apparent_temperature_min_C':'mean'
                   ,'sunshine_duration_minutes':'mean'
                   ,'sunshine_duration_hours':'mean'
                   ,'daylight_duration_minutes':'mean'
                   ,'daylight_duration_hours':'mean'
                   ,'weather_code':lambda x: x.mode().iloc[0] }

    my_dt =aggregate_dataframe(my_dt, groupby, agg_columns)


    #STORING THE DATA
    name_folder = 'Data/Gold/OpenMeteo/Historical/Calculations/PerYear'
    save_data_as_delta(my_dt, name_folder, mode="overwrite", layer='Gold',source='open-meteo-historical-daily', author='Augustin')




    #-------------------------------------------------------------------


    #-------------------------- 3.2.5. Means forecast per city per day -------------------------


    name_folder = 'Data/Gold/OpenMeteo/Forecast/Hourly'
    my_dt = DeltaTable(name_folder).to_pandas()

    groupby = ['requested_date', 'city', 'forecast_date']
    agg_columns = {'temperature_2m_C': 'mean',
        'relative_humidity_2m_inPercent': 'mean',
        'apparent_temperature_C': 'mean',
        'wind_speed_10m_kmh': 'mean',
        'precipitation_mm': 'mean',
        'rain_mm': 'mean',
        'snowfall_cm': 'mean',
        'pressure_msl_hPa': 'mean',
        'cloud_cover_inPercent': 'mean',
        'visibility_m': 'mean',
        'soil_temperature_0cm_C': 'mean',
        'soil_moisture_0_to_1cm_m3m3': 'mean'
    }
    my_dt =aggregate_dataframe(my_dt, groupby, agg_columns)



    #STORING THE DATA
    name_folder = 'Data/Gold/OpenMeteo/Forecast/Calculations/PerCityPerDay'
    save_data_as_delta(my_dt, name_folder, mode="overwrite", layer='Gold',source='open-meteo-forecast-daily', author='Augustin')



    #-------------------------------------------------------------------


    #--------------------------3.2.6. Means forecast per city -------------------------


    name_folder = 'Data/Gold/OpenMeteo/Forecast/Hourly'
    my_dt = DeltaTable(name_folder).to_pandas()
    days_of_forecast =len(my_dt['forecast_date'].unique())


    groupby = ['requested_date', 'city']
    agg_columns = {'temperature_2m_C': 'mean',
        'relative_humidity_2m_inPercent': 'mean',
        'apparent_temperature_C': 'mean',
        'wind_speed_10m_kmh': 'mean',
        'precipitation_mm': 'mean',
        'rain_mm': 'mean',
        'snowfall_cm': 'mean',
        'pressure_msl_hPa': 'mean',
        'cloud_cover_inPercent': 'mean',
        'visibility_m': 'mean',
        'soil_temperature_0cm_C': 'mean',
        'soil_moisture_0_to_1cm_m3m3': 'mean'
    }

    my_dt =aggregate_dataframe(my_dt, groupby, agg_columns)

    #insering the nb of day of forecast
    my_dt.insert(2,'nb_day_of_forecast',days_of_forecast)


    #STORING THE DATA
    name_folder = 'Data/Gold/OpenMeteo/Forecast/Calculations/PerCity'
    save_data_as_delta(my_dt, name_folder, mode="overwrite", layer='Gold',source='open-meteo-forecast-daily', author='Augustin')


    #-------------------------------------------------------------------


    #--------------------------CHECKING -------------------------
    # ------ CHECK GOLD TABLE STATS AND COMPARING WITH SILVER: Rows, Nulls, Duplicates ---------

    #Checking gold layer
    name_folder = 'Data/_meta/metadata_table'
    my_dt = DeltaTable(name_folder).to_pandas()
    my_dt = my_dt[my_dt['layer']=='Gold']

    #Comparating gold and silver
    name_folder = 'Data/_meta/metadata_table'
    my_dt = DeltaTable(name_folder).to_pandas()
    my_dt = my_dt[(my_dt['layer'] == 'Gold') | (my_dt['layer'] == 'Silver')]
    row_counts_per_table = pd.DataFrame({
        "layer":my_dt["layer"],
        "table_name": my_dt["table_name"],
        "table_path": my_dt["table_path"],
        "total_rows": my_dt['total_rows'],
        "rows_with_at_least_one_nulls":my_dt['rows_with_nulls'],
        "rows_duplicated":my_dt['rows_duplicated']
    })

    #row_counts_per_table.sort_values(by='table_path').head(30)

    print('script gold executed')
    export_metadata_to_excel(layer='Gold')

if __name__ == "__main__":
    main()

