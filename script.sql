
CREATE TABLE AIRLINES (
    airline_code VARCHAR(10) PRIMARY KEY,
    airline_name VARCHAR(100)
);

CREATE TABLE AIRPORTS (
    iata_code VARCHAR(3) PRIMARY KEY,
    city VARCHAR(50),
    state VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT
);

CREATE TABLE FLIGHTS (
    flight_id INTEGER PRIMARY KEY,
    flight_date DATE,
    flight_number VARCHAR(10),
    dep_time TIMESTAMP,
    dep_delay INTEGER,
    cancelled BOOLEAN,
    airline_code VARCHAR(10),
    origin_airport VARCHAR(3),
    dest_airport VARCHAR(3),
    FOREIGN KEY (airline_code) REFERENCES AIRLINES(airline_code),
    FOREIGN KEY (origin_airport) REFERENCES AIRPORTS(iata_code),
    FOREIGN KEY (dest_airport) REFERENCES AIRPORTS(iata_code)
);

CREATE TABLE WEATHER (
    weather_id INTEGER PRIMARY KEY,
    reading_time TIMESTAMP,
    wind_speed FLOAT,
    temperature FLOAT,
    airport_code VARCHAR(3),
    FOREIGN KEY (airport_code) REFERENCES AIRPORTS(iata_code)
);