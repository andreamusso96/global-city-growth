WITH county_population AS (
    SELECT  "GISJOIN" AS county_id,
            CAST("DATAYEAR" AS INT) AS year,
            CAST("CL8AA" AS FLOAT) AS population
    FROM {{ source('suburbanization', 'usa_county_population_raw') }}
)
SELECT  county_id,
        year,
        SUM(population) AS population
FROM county_population
WHERE year IN (2010, 2020)
GROUP BY county_id, year
