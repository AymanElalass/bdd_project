import sqlite3
import pandas as pd
import os
import sys

# database file names
db_name = "project_database.db"
sql_script = "script.sql"

# list of required files and their sources (for the user)
required_files = {
    "airlines.csv": "https://www.kaggle.com/datasets/usdot/flight-delays",
    "airports.csv": "https://www.kaggle.com/datasets/usdot/flight-delays",
    "flights.csv": "https://www.kaggle.com/datasets/usdot/flight-delays",
    "wind_speed.csv": "https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data",
    "temperature.csv": "https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data"
}

def check_files():
    """
    verifies that the necessary csv files are present in the directory.
    since we don't upload them to github (too large), we must warn the user.
    """
    print("checking data files...")
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print("\nerror: missing csv files!")
        print("please download the following files and place them in the root folder:")
        for f in missing_files:
            print(f"- {f} (source: {required_files[f]})")
        print("\nexiting program.")
        sys.exit(1)
    print("all csv files found. starting process...\n")

def create_database():
    # create tables using the sql script
    print("creating database...")
    
    # delete the old database if it exists to start fresh
    # useful when testing so we don't get duplicate error messages
    if os.path.exists(db_name):
        os.remove(db_name)
    
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    with open(sql_script, 'r') as f:
        sql_commands = f.read()
    
    cursor.executescript(sql_commands)
    conn.commit()
    conn.close()
    print("tables created")

def populate_flights_and_others():
    conn = sqlite3.connect(db_name)
    
    # load airlines
    print("loading airlines...")
    try:
        df_airlines = pd.read_csv("airlines.csv")
        # only keeping columns relevant to our schema to save memory
        df_airlines = df_airlines[['IATA_CODE', 'AIRLINE']]
        df_airlines.columns = ['airline_code', 'airline_name']
        df_airlines.to_sql('AIRLINES', conn, if_exists='append', index=False)
    except Exception as e:
        print(f"error loading airlines: {e}")
    
    # load airports
    print("loading airports...")
    try:
        df_airports = pd.read_csv("airports.csv")
        df_airports = df_airports[['IATA_CODE', 'CITY', 'STATE', 'LATITUDE', 'LONGITUDE']]
        df_airports.columns = ['iata_code', 'city', 'state', 'latitude', 'longitude']
        df_airports.to_sql('AIRPORTS', conn, if_exists='append', index=False)
    except Exception as e:
        print(f"error loading airports: {e}")

    # load flights
    # we only read the first 100k rows to avoid memory issues
    # the full dataset is huge and this is enough for the project
    print("loading flights (chunk of 100k)...")
    try:
        df_flights = pd.read_csv("flights.csv", low_memory=False, nrows=100000)
        
        # function to fix time format
        # the csv has weird formats like '2400' which is not valid in sql
        def format_time(x):
            if pd.isnull(x): return None
            s = str(int(x)).zfill(4)
            if s == '2400': return '23:59:00'
            return f"{s[:2]}:{s[2:]}:00"

        # preparing date and time columns
        # merging year/month/day columns into a single datetime object
        df_flights['flight_date'] = pd.to_datetime(df_flights[['YEAR', 'MONTH', 'DAY']])
        df_flights['dep_time_str'] = df_flights['DEPARTURE_TIME'].apply(format_time)
        df_flights['dep_time'] = pd.to_datetime(
            df_flights['flight_date'].astype(str) + ' ' + df_flights['dep_time_str'], 
            errors='coerce'
        )

        df_final = pd.DataFrame({
            'flight_date': df_flights['flight_date'],
            'flight_number': df_flights['FLIGHT_NUMBER'],
            'dep_time': df_flights['dep_time'],
            'dep_delay': df_flights['DEPARTURE_DELAY'].fillna(0),
            'cancelled': df_flights['CANCELLED'],
            'airline_code': df_flights['AIRLINE'],
            'origin_airport': df_flights['ORIGIN_AIRPORT'],
            'dest_airport': df_flights['DESTINATION_AIRPORT']
        })

        # check referential integrity
        # crucial step: we remove flights where the airport is not in our airports table
        # otherwise sqlite will throw a foreign key error
        existing_airports = set(pd.read_sql("SELECT iata_code FROM AIRPORTS", conn)['iata_code'])
        df_final = df_final[df_final['origin_airport'].isin(existing_airports)]
        df_final = df_final[df_final['dest_airport'].isin(existing_airports)]

        df_final.to_sql('FLIGHTS', conn, if_exists='append', index=False)
        print(f"{len(df_final)} flights loaded")
        
    except Exception as e:
        print(f"error loading flights: {e}")

    conn.commit()
    conn.close()

def process_weather_data():
    print("processing and loading weather data...")
    conn = sqlite3.connect(db_name)
    
    try:
        df_wind = pd.read_csv("wind_speed.csv")
        df_temp = pd.read_csv("temperature.csv")
        
        # dictionary to map city names to iata codes
        # weather data uses city names but our db is built on airport codes
        # we chose the main airports for these major cities
        city_to_iata = {
            'New York': 'JFK', 'Los Angeles': 'LAX', 'Chicago': 'ORD',
            'Atlanta': 'ATL', 'Dallas': 'DFW', 'Denver': 'DEN',
            'San Francisco': 'SFO', 'Seattle': 'SEA', 'Miami': 'MIA',
            'Boston': 'BOS', 'Phoenix': 'PHX', 'Detroit': 'DTW',
            'Houston': 'IAH', 'Minneapolis': 'MSP', 'Philadelphia': 'PHL'
        }

        # formatting the dataframes
        # transforming the data from wide format (columns per city) to long format (rows)
        df_wind_melted = pd.melt(df_wind, id_vars=['datetime'], var_name='City', value_name='wind_speed')
        df_temp_melted = pd.melt(df_temp, id_vars=['datetime'], var_name='City', value_name='temperature')
        
        # merging wind and temperature
        df_weather = pd.merge(df_wind_melted, df_temp_melted, on=['datetime', 'City'])
        
        # filter cities we are interested in
        # we discard cities that are not in our dictionary
        df_weather = df_weather[df_weather['City'].isin(city_to_iata.keys())].copy()
        df_weather = df_weather.dropna()
        
        df_weather['airport_code'] = df_weather['City'].map(city_to_iata)
        
        # converting kelvin to celsius
        # easier to read and analyze later
        df_weather['temperature'] = df_weather['temperature'] - 273.15 

        # removing duplicates just in case (same city same time)
        df_weather = df_weather.drop_duplicates(subset=['datetime', 'City'])

        # filtering outliers (sensor errors)
        # findings from our data exploration notebook: some sensors report extreme values
        # we keep realistic temperatures between -60 and 60 celsius
        df_weather = df_weather[
            (df_weather['temperature'] > -60) & 
            (df_weather['temperature'] < 60)
        ]

        # wind speed cannot be negative
        df_weather = df_weather[df_weather['wind_speed'] >= 0]

        df_final_weather = pd.DataFrame({
            'reading_time': pd.to_datetime(df_weather['datetime']),
            'wind_speed': df_weather['wind_speed'],
            'temperature': df_weather['temperature'],
            'airport_code': df_weather['airport_code']
        })
        
        # ensure airports exist in database
        # double checking to prevent foreign key errors with the weather table
        existing_airports = set(pd.read_sql("SELECT iata_code FROM AIRPORTS", conn)['iata_code'])
        df_final_weather = df_final_weather[df_final_weather['airport_code'].isin(existing_airports)]

        df_final_weather.to_sql('WEATHER', conn, if_exists='append', index=False)
        print(f"{len(df_final_weather)} weather records loaded")

    except Exception as e:
        print(f"error processing weather: {e}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    check_files()               # 1. Check if CSVs exist
    create_database()           # 2. Create tables
    populate_flights_and_others() # 3. Fill basic data
    process_weather_data()      # 4. Process and fill weather
    print("done. database is ready")