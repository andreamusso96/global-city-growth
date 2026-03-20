{% set analysis_id = var('constants')['MAIN_ANALYSIS_ID'] %}

WITH country_urbanization AS (
    SELECT  r.micro_region,
            u.year,
            u.urban_population_share,
            p.population
    FROM {{ ref('world_m49_region') }} r
    INNER JOIN {{ ref('world_urbanization') }} u
    USING (country)
    INNER JOIN {{ ref('world_population') }} p
    USING (country, year)
    WHERE u.year >= 1975
    AND u.year <= 2025
),
micro_region_urbanization AS (
    SELECT  {{ analysis_id }} AS analysis_id,
            micro_region,
            year,
            SUM(urban_population_share * population) / SUM(population) AS urban_population_share,
            SUM(population) AS population
    FROM country_urbanization
    GROUP BY micro_region, year
)
SELECT *
FROM micro_region_urbanization
ORDER BY micro_region, year
