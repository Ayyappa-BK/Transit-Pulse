-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create the bus_history table
CREATE TABLE IF NOT EXISTS bus_history (
    id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50) NOT NULL,
    route_id VARCHAR(50),
    direction_id VARCHAR(50),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    geom GEOMETRY(POINT, 4326),
    speed DOUBLE PRECISION,
    is_delayed BOOLEAN DEFAULT FALSE,
    weather_condition VARCHAR(100),
    temperature DOUBLE PRECISION
);

-- Create a spatial index for faster queries
CREATE INDEX idx_bus_history_geom ON bus_history USING GIST (geom);
