{{ config(
    indexes=[
      {'columns': ['centroid'], 'type': 'gist'},
      {'columns': ['cluster_id']}
    ]
) }}

WITH usa_base AS (
    SELECT  cluster_id,
            y1,
            y2,
            population_y1
    FROM {{ ref('world_cluster_growth_population_country_analysis') }}
    WHERE analysis_id = 1
    AND country = 'USA'
    AND y1 = 2010
    AND y2 = 2020
)
SELECT  ub.cluster_id,
        ub.y1,
        ub.y2,
        ub.population_y1,
        ST_Centroid(cg.geom) AS centroid
FROM usa_base ub
INNER JOIN {{ ref('world_cluster_growth_geom') }} cg
USING (cluster_id, y1, y2)
