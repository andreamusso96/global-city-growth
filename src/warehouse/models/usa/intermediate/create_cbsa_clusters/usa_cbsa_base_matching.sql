WITH left_geom AS (
    SELECT  cluster_id AS left_cluster_id,
            urban_threshold,
            geom
    FROM {{ ref('usa_cbsa_base_geom') }}
    WHERE year = 2010
),
right_geom AS (
    SELECT  cluster_id AS right_cluster_id,
            urban_threshold,
            geom
    FROM {{ ref('usa_cbsa_base_geom') }}
    WHERE year = 2020
),
matching AS (
    SELECT  l.left_cluster_id,
            r.right_cluster_id,
            2010 AS y1,
            2020 AS y2,
            l.urban_threshold
    FROM left_geom l
    LEFT JOIN right_geom r
    ON l.urban_threshold = r.urban_threshold
    AND r.geom && l.geom
    AND ST_Intersects(r.geom, l.geom)
    AND ST_Intersects(ST_Buffer(r.geom, -1000.0), ST_Buffer(l.geom, -1000.0))
)
SELECT *
FROM matching
