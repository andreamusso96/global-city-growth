WITH augmented_population AS (
    SELECT  augmented_cluster_id AS cluster_id,
            country,
            y1 AS year,
            SUM(population_y1) AS population_y1,
            SUM(population_y2) AS population_y2
    FROM {{ ref('world_suburbanization_augmented_mapping') }}
    GROUP BY augmented_cluster_id, country, y1
)
SELECT  cluster_id,
        country,
        year,
        population_y1,
        population_y2,
        LOG(population_y1) AS log_population,
        LOG(population_y2 / population_y1) AS log_growth
FROM augmented_population
WHERE population_y1 > 0
AND population_y2 > 0
