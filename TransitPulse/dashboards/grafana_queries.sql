-- Query 1: Average Speed per Route (last 1 hour)
SELECT
  $__timeGroupAlias(timestamp, '5m'),
  route_id,
  AVG(speed) as avg_speed
FROM bus_history
WHERE $__timeFilter(timestamp)
GROUP BY 1, 2
ORDER BY 1;

-- Query 2: Live Bus Map (Geom)
-- Configure Grafana > Geomap Panel
-- Layer Type: Markers/Route
-- Location Mode: Auto (if using Geom column) or Coords
-- If Coords:
SELECT
  timestamp as time,
  vehicle_id,
  route_id,
  latitude,
  longitude,
  speed,
  weather_condition
FROM bus_history
WHERE $__timeFilter(timestamp) 
  AND timestamp > NOW() - INTERVAL '5 minutes'
ORDER BY timestamp DESC;

-- If Geom column supported by datasource:
SELECT
  timestamp,
  geom,
  vehicle_id
FROM bus_history
WHERE $__timeFilter(timestamp);
