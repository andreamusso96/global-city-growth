WITH base AS (
    SELECT  'base' AS definition,
            y1 AS year,
            LOG(SUM(population_y2) / SUM(population_y1)) AS log_average_growth
    FROM {{ ref('world_cluster_growth_population_country_analysis') }}
    WHERE analysis_id = 1
    AND country = 'USA'
    AND y1 = 2010
    AND y2 = 2020
    GROUP BY y1
),
density AS (
    SELECT  'density' AS definition,
            y1 AS year,
            LOG(SUM(population_y2) / SUM(population_y1)) AS log_average_growth
    FROM {{ ref('usa_cluster_growth_population_analysis') }}
    WHERE analysis_id = 1
    AND y1 = 2010
    AND y2 = 2020
    GROUP BY y1
),
cbsa AS (
    SELECT  'cbsa' AS definition,
            y1 AS year,
            LOG(SUM(population_y2) / SUM(population_y1)) AS log_average_growth
    FROM {{ ref('usa_cbsa_growth_population') }}
    WHERE y1 = 2010
    AND y2 = 2020
    GROUP BY y1
)
SELECT *
FROM base
UNION ALL
SELECT *
FROM density
UNION ALL
SELECT *
FROM cbsa
