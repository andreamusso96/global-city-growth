{{ config(
    indexes=[
      {'columns': ['cluster_id']}
    ]
) }}

WITH usa_base AS (
    SELECT *
    FROM {{ ref('usa_suburbanization_base_centroids') }}
),
usa_base_small AS (
    SELECT *
    FROM usa_base
    WHERE population_y1 < 1000000
),
candidate_neighbors AS (
    SELECT  s.cluster_id,
            s.y1 AS year,
            s.population_y1,
            n.cluster_id AS neighbor_cluster_id,
            n.population_y1 AS neighbor_population_y1,
            ST_Distance(s.centroid, n.centroid) AS distance
    FROM usa_base_small s
    LEFT JOIN usa_base n
    ON s.cluster_id != n.cluster_id
    AND n.centroid && ST_Expand(s.centroid, 100000)
    AND ST_DWithin(s.centroid, n.centroid, 100000)
),
ranked_neighbors AS (
    SELECT  *,
            ROW_NUMBER() OVER (
                PARTITION BY cluster_id
                ORDER BY neighbor_population_y1 DESC NULLS LAST, distance ASC NULLS LAST
            ) AS neighbor_rank
    FROM candidate_neighbors
),
largest_neighbor AS (
    SELECT *
    FROM ranked_neighbors
    WHERE neighbor_rank = 1
)
SELECT  cluster_id,
        year,
        population_y1,
        COALESCE(neighbor_population_y1 > 1000000, FALSE) AS has_large_nbr,
        neighbor_cluster_id,
        neighbor_population_y1,
        distance
FROM largest_neighbor
