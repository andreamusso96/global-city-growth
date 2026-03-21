{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
      {'columns': ['centroid'], 'type': 'gist'},
      {'columns': ['country', 'y1', 'y2', 'cluster_id']}
    ]
) }}

WITH restricted_base_clusters AS (
    SELECT  pop.cluster_id,
            pop.country,
            pop.y1,
            pop.y2,
            pop.population_y1,
            pop.population_y2,
            geom.geom
    FROM {{ ref('world_cluster_growth_population_country_analysis') }} pop
    INNER JOIN {{ ref('world_urbanization_groups') }} ug
    USING (country)
    INNER JOIN {{ ref('world_cluster_growth_geom') }} geom
    ON pop.cluster_id = geom.cluster_id
    AND pop.y1 = geom.y1
    AND pop.y2 = geom.y2
    WHERE pop.analysis_id = 1
    AND geom.urban_threshold = 21
    AND pop.y2 = pop.y1 + 10
    AND ug.urban_population_share_group = '60-100'
)
SELECT  cluster_id,
        country,
        y1,
        y2,
        population_y1,
        population_y2,
        geom,
        ST_Centroid(geom) AS centroid,
        ST_Area(geom) AS area
FROM restricted_base_clusters
WHERE geom IS NOT NULL
AND NOT ST_IsEmpty(geom)
AND population_y1 > 0
AND population_y2 > 0
