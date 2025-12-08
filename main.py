import sqlite3
import pandas as pd
import os
import sys

# Database file names
db_name = "project_database.db"
sql_script = "script.sql"

# List of required files and their sources
# Now expecting files at the ROOT of the project
required_files = {
    "airlines.csv": "https://www.kaggle.com/datasets/usdot/flight-delays",
    "airports.csv": "https://www.kaggle.com/datasets/usdot/flight-delays",
    "flights.csv": "https://www.kaggle.com/datasets/usdot/flight-delays",
    "wind_speed.csv": "https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data",
    "temperature.csv": "https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data"
}

def check_files():
    """
    Verifies that the necessary CSV files are present in the root directory.
    """
    print("Checking data files...")
    missing_files = []
    
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print("\nError: Missing CSV files!")
        print("Please download the following files and place them in the root folder:")
        for f in missing_files:
            print(f"- {f} (Source: {required_files[f]})")
        print("\nExiting program.")
        sys.exit(1)
    print("All CSV files found. Starting process...\n")

def create_database():
    print("Creating database...")
    
    if os.path.exists(db_name):
        os.remove(db_name)
    
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    try:
        with open(sql_script, 'r') as f:
            sql_commands = f.read()
        cursor.executescript(sql_commands)
        conn.commit()
        print("Tables created.")
    except FileNotFoundError:
        print(f"Error: Could not find {sql_script}. Make sure it is in the root directory.")
        sys.exit(1)
    finally:
        conn.close()

def populate_flights_and_others():
    conn = sqlite3.connect(db_name)
    
    # Load Airlines
    print("Loading airlines...")
    try:
        df_airlines = pd.read_csv("airlines.csv")
        df_airlines = df_airlines[['IATA_CODE', 'AIRLINE']]
        df_airlines.columns = ['airline_code', 'airline_name']
        df_airlines.to_sql('AIRLINES', conn, if_exists='append', index=False)
    except Exception as e:
        print(f"Error loading airlines: {e}")
    
    # Load Airports
    print("Loading airports...")
    try:
        df_airports = pd.read_csv("airports.csv")
        df_airports = df_airports[['IATA_CODE', 'CITY', 'STATE', 'LATITUDE', 'LONGITUDE']]
        df_airports.columns = ['iata_code', 'city', 'state', 'latitude', 'longitude']
        df_airports.to_sql('AIRPORTS', conn, if_exists='append', index=False)
    except Exception as e:
        print(f"Error loading airports: {e}")

    # Load Flights
    print("Loading flights (chunk of 100k)...")
    try:
        # We only read the first 100k rows to be fast for the script
        df_flights = pd.read_csv("flights.csv", low_memory=False, nrows=100000)
        
        def format_time(x):
            if pd.isnull(x): return None
            s = str(int(x)).zfill(4)
            if s == '2400': return '23:59:00'
            return f"{s[:2]}:{s[2:]}:00"

        df_flights['flight_date'] = pd.to_datetime(df_flights[['YEAR', 'MONTH', 'DAY']]).dt.date
        df_flights['dep_time'] = df_flights['DEPARTURE_TIME'].apply(format_time)

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

        # Referential Integrity
        existing_airports = set(pd.read_sql("SELECT iata_code FROM AIRPORTS", conn)['iata_code'])
        df_final = df_final[df_final['origin_airport'].isin(existing_airports)]
        df_final = df_final[df_final['dest_airport'].isin(existing_airports)]

        df_final.to_sql('FLIGHTS', conn, if_exists='append', index=False)
        print(f"{len(df_final)} flights loaded.")
        
    except Exception as e:
        print(f"Error loading flights: {e}")

    conn.commit()
    conn.close()

def process_weather_data():
    print("Processing and loading weather data...")
    conn = sqlite3.connect(db_name)
    
    try:
        df_wind = pd.read_csv("wind_speed.csv")
        df_temp = pd.read_csv("temperature.csv")
        
        city_to_iata = {
            'New York': 'JFK', 'Los Angeles': 'LAX', 'Chicago': 'ORD',
            'Atlanta': 'ATL', 'Dallas': 'DFW', 'Denver': 'DEN',
            'San Francisco': 'SFO', 'Seattle': 'SEA', 'Miami': 'MIA',
            'Boston': 'BOS', 'Phoenix': 'PHX', 'Detroit': 'DTW',
            'Houston': 'IAH', 'Minneapolis': 'MSP', 'Philadelphia': 'PHL'
        }

        df_wind_melted = pd.melt(df_wind, id_vars=['datetime'], var_name='City', value_name='wind_speed')
        df_temp_melted = pd.melt(df_temp, id_vars=['datetime'], var_name='City', value_name='temperature')
        
        df_weather = pd.merge(df_wind_melted, df_temp_melted, on=['datetime', 'City'])
        
        df_weather = df_weather[df_weather['City'].isin(city_to_iata.keys())].copy().dropna()
        df_weather['airport_code'] = df_weather['City'].map(city_to_iata)
        
        # Kelvin to Celsius
        df_weather['temperature'] = df_weather['temperature'] - 273.15 
        df_weather = df_weather.drop_duplicates(subset=['datetime', 'City'])

        # Outlier filtering
        df_weather = df_weather[(df_weather['temperature'] > -60) & (df_weather['temperature'] < 60)]
        df_weather = df_weather[df_weather['wind_speed'] >= 0]

        df_final_weather = pd.DataFrame({
            'reading_time': pd.to_datetime(df_weather['datetime']),
            'wind_speed': df_weather['wind_speed'],
            'temperature': df_weather['temperature'],
            'airport_code': df_weather['airport_code']
        })
        
        existing_airports = set(pd.read_sql("SELECT iata_code FROM AIRPORTS", conn)['iata_code'])
        df_final_weather = df_final_weather[df_final_weather['airport_code'].isin(existing_airports)]

        df_final_weather.to_sql('WEATHER', conn, if_exists='append', index=False)
        print(f"{len(df_final_weather)} weather records loaded.")

    except Exception as e:
        print(f"Error processing weather: {e}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    check_files()               # 1. Check if CSVs exist at root
    create_database()           # 2. Create tables
    populate_flights_and_others() # 3. Fill basic data
    process_weather_data()      # 4. Process and fill weather
    print("Done. Database is ready.")