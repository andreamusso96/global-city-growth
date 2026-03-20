WITH base AS (
    SELECT  'base' AS definition,
            cluster_id,
            y1 AS year,
            LOG(population_y1) AS log_population,
            LOG(population_y2 / population_y1) AS log_growth
    FROM {{ ref('world_cluster_growth_population_country_analysis') }}
    WHERE analysis_id = 1
    AND country = 'USA'
    AND y1 = 2010
    AND y2 = 2020
),
density AS (
    SELECT  'density' AS definition,
            cluster_id,
            y1 AS year,
            LOG(population_y1) AS log_population,
            LOG(population_y2 / population_y1) AS log_growth
    FROM {{ ref('usa_cluster_growth_population_analysis') }}
    WHERE analysis_id = 1
    AND y1 = 2010
    AND y2 = 2020
),
cbsa AS (
    SELECT  'cbsa' AS definition,
            cluster_id,
            y1 AS year,
            LOG(population_y1) AS log_population,
            LOG(population_y2 / population_y1) AS log_growth
    FROM {{ ref('usa_cbsa_growth_population') }}
    WHERE y1 = 2010
    AND y2 = 2020
)
SELECT *
FROM base
UNION ALL
SELECT *
FROM density
UNION ALL
SELECT *
FROM cbsa
