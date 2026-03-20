WITH county_population_y1 AS (
    SELECT  cw.cluster_id,
            cg.y1,
            cg.y2,
            cg.urban_threshold,
            SUM(cp.population) AS population_y1
    FROM {{ ref('usa_crosswalk_county_to_cbsa_growth') }} cw
    INNER JOIN {{ ref('usa_cbsa_growth_geom') }} cg
    USING (cluster_id)
    INNER JOIN {{ ref('usa_county_population') }} cp
    ON cw.county_id = cp.county_id
    AND cp.year = cg.y1
    GROUP BY cw.cluster_id, cg.y1, cg.y2, cg.urban_threshold
),
county_population_y2 AS (
    SELECT  cw.cluster_id,
            cg.y1,
            cg.y2,
            cg.urban_threshold,
            SUM(cp.population) AS population_y2
    FROM {{ ref('usa_crosswalk_county_to_cbsa_growth') }} cw
    INNER JOIN {{ ref('usa_cbsa_growth_geom') }} cg
    USING (cluster_id)
    INNER JOIN {{ ref('usa_county_population') }} cp
    ON cw.county_id = cp.county_id
    AND cp.year = cg.y2
    GROUP BY cw.cluster_id, cg.y1, cg.y2, cg.urban_threshold
)
SELECT  p1.cluster_id,
        p1.y1,
        p1.y2,
        p1.urban_threshold,
        p1.population_y1,
        p2.population_y2
FROM county_population_y1 p1
INNER JOIN county_population_y2 p2
USING (cluster_id, y1, y2, urban_threshold)
