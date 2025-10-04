-- -- 1. Get the first 10 superheroes
-- SELECT * FROM superhero LIMIT 10;

-- -- 2. Get up to 50 Marvel Comics superheroes (id, name, full name, publisher), ordered alphabetically
-- SELECT id, superhero_name, full_name, publisher_id
-- FROM superhero
-- WHERE publisher_id = (SELECT id FROM publisher WHERE publisher_name = 'Marvel Comics')
-- ORDER BY superhero_name
-- LIMIT 50;

-- -- 3. Count total superheroes, those with full name, those with publisher, and how many have NULL height
-- SELECT COUNT(*) AS total,
--        COUNT(full_name) AS full_name_not_null,
--        COUNT(publisher_id) AS publisher_not_null,
--        SUM(CASE WHEN height_cm IS NULL THEN 1 ELSE 0 END) AS height_null_count
-- FROM superhero;

-- -- 4. Show number of superheroes per publisher, sorted by hero count (descending)
-- SELECT p.publisher_name, COUNT(s.id) AS hero_count
-- FROM publisher p
-- LEFT JOIN superhero s ON s.publisher_id = p.id
-- GROUP BY p.publisher_name
-- ORDER BY hero_count DESC;

-- -- 5. Show average height and weight of superheroes by publisher
-- SELECT p.publisher_name,
--        AVG(s.height_cm)::numeric(10,2) AS avg_height_cm,
--        AVG(s.weight_kg)::numeric(10,2) AS avg_weight_kg
-- FROM publisher p
-- LEFT JOIN superhero s ON s.publisher_id = p.id
-- GROUP BY p.publisher_name
-- ORDER BY avg_height_cm DESC;

-- -- 6. Show top 20 superheroes ranked by total attribute values
-- SELECT s.id, s.superhero_name, p.publisher_name,
--        COALESCE(SUM(ha.attribute_value), 0) AS total_attributes
-- FROM superhero s
-- LEFT JOIN hero_attribute ha ON s.id = ha.hero_id
-- LEFT JOIN publisher p ON s.publisher_id = p.id
-- GROUP BY s.id, s.superhero_name, p.publisher_name
-- ORDER BY total_attributes DESC
-- LIMIT 20;

-- -- 7. Show number of heroes with each power per publisher
-- SELECT sp.power_name,
--        p.publisher_name,
--        COUNT(DISTINCT hp.hero_id) AS heroes_with_power
-- FROM superpower sp
-- JOIN hero_power hp ON sp.id = hp.power_id
-- JOIN superhero s ON hp.hero_id = s.id
-- JOIN publisher p ON s.publisher_id = p.id
-- GROUP BY sp.power_name, p.publisher_name
-- ORDER BY sp.power_name, heroes_with_power DESC;

-- -- 8. Show average attribute values per publisher and attribute
-- SELECT a.attribute_name, p.publisher_name,
--        AVG(ha.attribute_value)::numeric(8,2) AS avg_value
-- FROM attribute a
-- JOIN hero_attribute ha ON a.id = ha.attribute_id
-- JOIN superhero s ON ha.hero_id = s.id
-- JOIN publisher p ON s.publisher_id = p.id
-- GROUP BY a.attribute_name, p.publisher_name
-- ORDER BY a.attribute_name, avg_value DESC;

-- -- 9. Show number of superheroes by publisher and gender
-- SELECT p.publisher_name, g.gender, COUNT(s.id) AS cnt
-- FROM publisher p
-- LEFT JOIN superhero s ON s.publisher_id = p.id
-- LEFT JOIN gender g ON s.gender_id = g.id
-- GROUP BY p.publisher_name, g.gender
-- ORDER BY p.publisher_name, cnt DESC;

-- -- 10. Get top 10 tallest superheroes
-- SELECT id, superhero_name, height_cm
-- FROM superhero
-- WHERE height_cm IS NOT NULL
-- ORDER BY height_cm DESC
-- LIMIT 10;


-- SELECT id, superhero_name, weight_kg
-- FROM superhero
-- WHERE weight_kg IS NOT NULL
-- ORDER BY weight_kg DESC
-- LIMIT 10;


-- SELECT r.race, COUNT(s.id) AS cnt
-- FROM race r
-- LEFT JOIN superhero s ON s.race_id = r.id
-- GROUP BY r.race
-- ORDER BY cnt DESC;


-- SELECT s.id, s.superhero_name, COUNT(hp.power_id) AS power_count
-- FROM superhero s
-- LEFT JOIN hero_power hp ON s.id = hp.hero_id
-- GROUP BY s.id, s.superhero_name
-- ORDER BY power_count DESC
-- LIMIT 20;


-- SELECT s.id, s.superhero_name
-- FROM superhero s
-- LEFT JOIN hero_attribute ha ON s.id = ha.hero_id
-- LEFT JOIN hero_power hp ON s.id = hp.hero_id
-- WHERE ha.hero_id IS NULL AND hp.hero_id IS NULL
-- LIMIT 100;


-- SELECT s.id, s.superhero_name, p.publisher_name, ha.attribute_value AS intelligence
-- FROM attribute a
-- JOIN hero_attribute ha ON a.id = ha.attribute_id
-- JOIN superhero s ON ha.hero_id = s.id
-- LEFT JOIN publisher p ON s.publisher_id = p.id
-- WHERE a.attribute_name = 'Intelligence'
-- ORDER BY ha.attribute_value DESC
-- LIMIT 20;

-- -- 16. Compare average total attribute values by alignment (Good, Bad, Neutral)
-- WITH totals AS (
--   SELECT s.id, s.alignment_id, COALESCE(SUM(ha.attribute_value),0) AS total_attr
--   FROM superhero s
--   LEFT JOIN hero_attribute ha ON s.id = ha.hero_id
--   GROUP BY s.id, s.alignment_id
-- )
-- SELECT al.alignment, AVG(t.total_attr)::numeric(8,2) AS avg_total_attributes
-- FROM totals t
-- LEFT JOIN alignment al ON t.alignment_id = al.id
-- GROUP BY al.alignment
-- ORDER BY avg_total_attributes DESC;
