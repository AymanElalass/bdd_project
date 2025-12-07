
CREATE TABLE AIRLINES (
    airline_code CHAR(2) PRIMARY KEY,
    airline_name VARCHAR(100)
);

CREATE TABLE AIRPORTS (
    iata_code CHAR(3) PRIMARY KEY,
    airport_name VARCHAR(100) NOT NULL,
    city VARCHAR(50),
    state CHAR(2),
    latitude FLOAT,
    longitude FLOAT,
    
    CHECK(latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180)
);

CREATE TABLE FLIGHTS (
    flight_id INTEGER PRIMARY KEY,
    flight_date DATE,
    flight_number VARCHAR(10),
    dep_time TEXT,
    dep_delay INTEGER,
    cancelled BOOLEAN,
    airline_code CHAR(2),
    origin_airport CHAR(3),
    dest_airport CHAR(3),
    CHECK(dep_time GLOB '[0-2][0-9]:[0-5][0-9]:[0-5][0-9]')
    FOREIGN KEY (airline_code) 
        REFERENCES AIRLINES(airline_code),
    FOREIGN KEY (origin_airport) 
        REFERENCES AIRPORTS(iata_code),
    FOREIGN KEY (dest_airport) 
        REFERENCES AIRPORTS(iata_code)
);

CREATE TABLE WEATHER (
    weather_id INTEGER PRIMARY KEY,
    reading_time TIMESTAMP,
    wind_speed FLOAT,
    temperature FLOAT,
    airport_code CHAR(3),
    FOREIGN KEY (airport_code) REFERENCES AIRPORTS(iata_code)
);