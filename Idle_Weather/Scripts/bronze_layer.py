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

venv = os.environ.get('VIRTUAL_ENV') or sys.prefix
    #print(f"Environnement virtuel en cours : {venv}")
    #sys.exit("Message facultatif : arrêt du script.")

def main():


    #--------------------1. GEOLOCALISATION ------------------------
    list_of_cities = ['Buenos Aires', 'Rio de Janeiro', 'Marseille'
                       , 'Brussels', 'Namur',  'Montreal' 
                       ,'Barcelona','New York','Chicago'
                       ,'Sao Paulo','Toronto','Melbourne','London','Mexico City'
                       ,'Lima','La Paz', 'Boston', 'Kinshasa']


    my_geo_dict = {}
    my_geo_df = pd.DataFrame()
    for city in list_of_cities:

        #use of my own function from openmeteo_API module
        my_df = get_geolocation_openmeteo(city,nb_results=2)
        my_geo_df = pd.concat([my_geo_df,my_df])

    #deleting of useless and problematic column
    my_geo_df =my_geo_df.drop(columns=['postcodes'])

    #STORING THE DATA
    name_folder = 'Data/Bronze/OpenMeteo/Others/Geolocation'
    partition_cols = None
    predicate = "target.id = source.id"
    save_new_data_as_delta(my_geo_df,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Bronze', source= 'open-meteo-geoloc', author ='Augustin')
    print('geolocalisation stored')
    #time.sleep(20)
    #-------------------------------------------------------------------


    #--------------------------2. WEATHERCODE -------------------------
    weahter_code_data = {
        "Code": [
            "0", "1, 2, 3", "45, 48", "51, 53, 55", "56, 57", "61, 63, 65", 
            "66, 67", "71, 73, 75", "77", "80, 81, 82", "85, 86", "95 *", "96, 99 *"
        ],
        "Description": [
            "Clear sky",
            "Mainly clear, partly cloudy, overcast",
            "Fog, depositing rime fog",
            "Drizzle Light intensity, Drizzle Moderate intensity, Drizzle Dense intensity",
            "Freezing Drizzle Light intensity, Freezing Drizzle Dense intensity",
            "Rain Slight, Rain Moderate, Rain Heavy",
            "Freezing Rain Light, Freezing Rain Heavy",
            "Snow fall Slight,Snow fall Moderate, Snow fall Heavy ",
            "Snow grains",
            "Rain showers Slight,Rain showers Moderate,Rain showers Biolent",
            "Snow showers slight, Snow showers heavy",
            "Thunderstorm Slight or moderate",
            "Thunderstorm with slight hail,Thunderstorm with heavy hail"
        ]
    }
    df = pd.DataFrame(weahter_code_data)

    #STORING THE DATA
    name_folder = 'Data/Bronze/OpenMeteo/Others/WeatherCode'
    mode = 'overwrite'
    partition_cols = None
    save_data_as_delta(df,name_folder,mode=mode, partition_cols=partition_cols,layer = 'Bronze', source= 'open-meteo-weathercode', author ='Augustin')
    print('weather code stored')
    #time.sleep(20)
    #-------------------------------------------------------------------



    #---------------------- 3. CURRENT WEATHER ------------------------------------

    #We rethrieve the cities from which we have latitude and longitude informations (we will do the same in the rest of the notebook).
    name_folder = 'Data/Bronze/OpenMeteo/Others/Geolocation'
    my_dt = DeltaTable(name_folder).to_pandas()
    # as we can have severals results from one city name, we decide to only choose the result with the highest probability
    list_of_cities_name = list(my_dt.loc[my_dt['probability']==1, 'name'].unique())


    my_current_dict = {}
    my_current_df = pd.DataFrame()
    for city in list_of_cities_name:

        #use of my own function from openmeteo_API module
        my_df = get_current_weather(city)
        my_current_df = pd.concat([my_current_df,my_df])

    #STORING THE DATA
    name_folder = 'Data/Bronze/OpenMeteo/Current'
    predicate = "target.Date = source.Date AND target.Time = source.Time AND target.City = source.City"
    partition_cols = "Date"
    save_new_data_as_delta(my_current_df,name_folder,predicate = predicate, partition_cols=partition_cols, layer = 'Bronze', source= 'open-meteo-current', author ='Augustin')

    print('current stored')
    #time.sleep(20)
    #-------------------------------------------------------------------



    #---------------------- 4. FORECAST DAILY WEATHER ------------------------------------

    name_folder = 'Data/Bronze/OpenMeteo/Others/Geolocation'
    my_dt = DeltaTable(name_folder).to_pandas()
    list_of_cities_name = list(my_dt.loc[my_dt['probability']==1, 'name'].unique())


    my_forecast_daily_df = pd.DataFrame()
    for city in list_of_cities_name:

        #use of my own function from openmeteo_API module
        my_df = get_forecast_daily_weather(city, forecast_days=7)
        my_forecast_daily_df = pd.concat([my_forecast_daily_df,my_df])

    #STORING THE DATA
    name_folder = 'Data/Bronze/OpenMeteo/Forecast/Daily'
        #the predicate of our merge is on the requested_date, the city and the forecast_day.
    predicate = "target.Requested_Date = source.Requested_Date AND target.City = source.City and target.forecast_day = source.forecast_day "
    partition_cols = ["Requested_Date"]

    save_new_data_as_delta(my_forecast_daily_df,name_folder,predicate= predicate, partition_cols=partition_cols,layer = 'Bronze', source= 'open-meteo-forecast-daily', author ='Augustin')
    print('forecast daily stored')
    #time.sleep(20)
    #-------------------------------------------------------------------



    #---------------------- 5. FORECAST HOURLY WEATHER ------------------------------------

    name_folder = 'Data/Bronze/OpenMeteo/Others/Geolocation'
    my_dt = DeltaTable(name_folder).to_pandas()
    list_of_cities_name = list(my_dt.loc[my_dt['probability']==1, 'name'].unique())


    my_forecast_hourly_df = pd.DataFrame()
    for city in list_of_cities_name:
        
        #use of my own function from openmeteo_API module
        my_df = get_forecast_hourly_weather(city, forecast_days=5)
        my_forecast_hourly_df = pd.concat([my_forecast_hourly_df,my_df])


    #STORING THE DATA
    name_folder = 'Data/Bronze/OpenMeteo/Forecast/Hourly'
    predicate = """target.Requested_Date = source.Requested_Date  AND target.City = source.City AND target.Forecast_Date = source.Forecast_Date and target.Forecast_Hour = source.Forecast_Hour """
    partition_cols = ["Requested_Date"]
    upsert_data_as_delta(my_forecast_hourly_df,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Bronze', source= 'open-meteo-forecast-hourly', author ='Augustin')
    print('forecast hourly stored')
    time.sleep(70)
    #-------------------------------------------------------------------



    #---------------------- 6. HISTORICAL DAILY WEATHER ------------------------------------

    name_folder = 'Data/Bronze/OpenMeteo/Others/Geolocation'
    my_dt = DeltaTable(name_folder).to_pandas()
    list_of_cities_name = list(my_dt.loc[my_dt['probability']==1, 'name'].unique())

    my_historical_weather_df = pd.DataFrame()
    for city in list_of_cities_name:
        
        #use of my own function from openmeteo_API module
        my_df = get_daily_historical_weather(city,'2012-01-01','2012-12-31' )
        my_historical_weather_df = pd.concat([my_historical_weather_df,my_df])
        #adding a delay because severals times, the API stopped because of the limitation
        time.sleep(10)


    #STORING THE DATA
    name_folder = 'Data/Bronze/OpenMeteo/Historical/Daily'
    predicate = """target.City = source.City AND target.Historical_Date = source.Historical_Date"""
    partition_cols = ["Historical_Year"]
    save_new_data_as_delta(my_historical_weather_df,name_folder,predicate= predicate, partition_cols=partition_cols,layer = 'Bronze', source= 'open-meteo-historical-daily', author ='Augustin')
    print('historical daily stored')
    #-------------------------------------------------------------------



    #---------------------- 7. CHECK ------------------------------------
    name_folder = 'Data/_meta/metadata_table'
    my_dt = DeltaTable(name_folder).to_pandas()
    my_dt = my_dt[my_dt['layer']=='Bronze']

    print('script bronze executed')
    export_metadata_to_excel(layer='Bronze')
    """
    name_folder = 'Data/_meta/metadata_table'
    my_dt = DeltaTable(name_folder).to_pandas()
    my_dt = my_dt[my_dt['layer']=='Bronze']
    # Optionnel : afficher les résultats avec les noms des tables
    row_counts_per_table = pd.DataFrame({
            "layer":my_dt["layer"],
        "table_name": my_dt["table_name"],
        "table_path": my_dt["table_path"],
        "total_rows": my_dt['total_rows'],
        "rows_with_at_least_one_nulls":my_dt['rows_with_nulls'],
        "rows_duplicated":my_dt['rows_duplicated']
    })
    row_counts_per_table.head(10)


    """

if __name__ == "__main__":
    main()
