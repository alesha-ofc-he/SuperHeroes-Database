-- # Task 5: Geo Visualization Setup
-- 1. Create a table for Publisher Coordinates (using common HQ locations)
DROP TABLE IF EXISTS superhero.publisher_coords;
CREATE TABLE superhero.publisher_coords (
    publisher_id INT PRIMARY KEY,
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6)
);

INSERT INTO superhero.publisher_coords (publisher_id, latitude, longitude) VALUES
(13, 40.7128, -74.0060),    -- Marvel Comics (New York)
(4, 34.1485, -118.3245),    -- DC Comics (Burbank/LA Area)
(10, 45.5051, -122.6750),   -- Image Comics (Portland)
(3, 45.4542, -122.6710),    -- Dark Horse Comics (Milwaukie, OR)
(5, 37.7749, -122.4194),    -- George Lucas (San Francisco Area)
(15, 40.7128, -74.0060),    -- NBC - Heroes (NYC Area)
(17, 35.6895, 139.6917);    -- Shueisha (Tokyo, Japan)

-- Add a default/fallback coordinate for publishers not listed
INSERT INTO superhero.publisher_coords (publisher_id, latitude, longitude)
SELECT id, 37.090240, -95.712891 -- Center of USA for others
FROM superhero.publisher
WHERE id NOT IN (SELECT publisher_id FROM superhero.publisher_coords);


-- 2. Create a view to join superhero data with coordinates
DROP VIEW IF EXISTS superhero.hero_locations_view;
CREATE VIEW superhero.hero_locations_view AS
SELECT
    t1.superhero_name,
    t1.id AS hero_id,
    t3.publisher_name,
    t4.latitude,
    t4.longitude
FROM superhero.superhero t1
JOIN superhero.publisher t3 ON t1.publisher_id = t3.id
LEFT JOIN superhero.publisher_coords t4 ON t1.publisher_id = t4.publisher_id;


-- # Task 10: Data Engineering / Comparison Chart Setup
-- New table to log average Intelligence over time for comparison
DROP TABLE IF EXISTS superhero.attribute_log;
CREATE TABLE superhero.attribute_log (
  log_time TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
  avg_intelligence NUMERIC,
  source VARCHAR(10)
);

-- Insert initial STATIC data (for the 'CSV' equivalent line)
INSERT INTO superhero.attribute_log (log_time, avg_intelligence, source)
VALUES (NOW() - INTERVAL '10 minutes', 80.0, 'Static_CSV'),
       (NOW() - INTERVAL '5 minutes', 80.0, 'Static_CSV'),
       (NOW(), 80.0, 'Static_CSV');

-- Insert initial LIVE data (same value for the 'DB' line to start together)
INSERT INTO superhero.attribute_log (log_time, avg_intelligence, source)
VALUES (NOW() - INTERVAL '10 minutes', 80.0, 'Live_DB'),
       (NOW() - INTERVAL '5 minutes', 80.0, 'Live_DB'),
       (NOW(), 80.0, 'Live_DB');

COMMIT;