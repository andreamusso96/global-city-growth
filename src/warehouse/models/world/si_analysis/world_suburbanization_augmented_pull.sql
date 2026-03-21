WITH pull_inputs AS (
    SELECT  cluster_id,
            neighbor_id,
            country,
            y1,
            y2,
            distance,
            area,
            population_y1,
            population_y2,
            neighbor_population_y1,
            neighbor_population_y2,
            neighbor_population_y1 / GREATEST(distance, SQRT(area)) AS pull,
            population_y1 / SQRT(area) AS self_pull
    FROM {{ ref('world_suburbanization_augmented_neighbors') }}
    WHERE area > 0
    AND population_y1 > 0
    AND population_y2 > 0
    AND neighbor_population_y1 > 0
    AND neighbor_population_y2 > 0
)
SELECT  cluster_id,
        neighbor_id,
        country,
        y1,
        y2,
        distance,
        area,
        population_y1,
        population_y2,
        neighbor_population_y1,
        neighbor_population_y2,
        pull,
        self_pull,
        pull > self_pull AS should_merge
FROM pull_inputs
