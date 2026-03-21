WITH ranked_neighbors AS (
    SELECT  *,
            ROW_NUMBER() OVER (
                PARTITION BY cluster_id, country, y1, y2
                ORDER BY pull DESC, neighbor_population_y1 DESC, neighbor_id ASC
            ) AS neighbor_rank
    FROM {{ ref('world_suburbanization_augmented_pull') }}
),
top_neighbor AS (
    SELECT *
    FROM ranked_neighbors
    WHERE neighbor_rank = 1
)
SELECT  cluster_id,
        country,
        y1,
        y2,
        neighbor_id,
        distance,
        area,
        population_y1,
        population_y2,
        neighbor_population_y1,
        neighbor_population_y2,
        pull,
        self_pull,
        should_merge,
        CASE
            WHEN should_merge THEN neighbor_id
            ELSE cluster_id
        END AS augmented_cluster_id
FROM top_neighbor
