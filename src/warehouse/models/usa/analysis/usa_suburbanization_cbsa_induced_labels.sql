{{ config(
    indexes=[
      {'columns': ['cluster_id']},
      {'columns': ['cbsa_cluster_id']}
    ]
) }}

WITH usa_base AS (
    SELECT  p.cluster_id,
            p.y1,
            p.y2,
            p.population_y1,
            ST_Transform(g.geom, 5070) AS geom_5070
    FROM {{ ref('world_cluster_growth_population_country_analysis') }} p
    INNER JOIN {{ ref('world_cluster_growth_geom') }} g
    USING (cluster_id, y1, y2)
    WHERE p.analysis_id = 1
    AND p.country = 'USA'
    AND p.y1 = 2010
    AND p.y2 = 2020
    AND g.urban_threshold = 21
),
candidate_matches AS (
    SELECT  b.cluster_id,
            c.cluster_id AS cbsa_cluster_id,
            b.y1,
            b.y2,
            b.population_y1,
            ST_Area(ST_Intersection(b.geom_5070, c.geom)) AS intersection_area
    FROM usa_base b
    INNER JOIN {{ ref('usa_cbsa_growth_geom') }} c
    ON c.y1 = b.y1
    AND c.y2 = b.y2
    AND c.geom && b.geom_5070
    AND ST_Intersects(c.geom, b.geom_5070)
),
ranked_matches AS (
    SELECT  *,
            ROW_NUMBER() OVER (
                PARTITION BY cluster_id
                ORDER BY intersection_area DESC, cbsa_cluster_id ASC
            ) AS match_rank
    FROM candidate_matches
)
SELECT  cluster_id,
        cbsa_cluster_id,
        y1,
        y2,
        population_y1,
        intersection_area
FROM ranked_matches
WHERE match_rank = 1
