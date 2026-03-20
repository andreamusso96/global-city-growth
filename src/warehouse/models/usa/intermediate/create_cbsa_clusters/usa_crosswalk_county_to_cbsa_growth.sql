WITH county_geom AS (
    SELECT  county_id,
            county_geoid,
            state_fips,
            geom,
            ST_Area(geom) AS area_county
    FROM {{ ref('usa_county_geom') }}
    WHERE state_fips != '72'
),
cbsa_county_intersection AS (
    SELECT  cg.cluster_id,
            c.county_id,
            c.county_geoid,
            c.area_county,
            ST_Area(ST_Intersection(cg.geom, c.geom)) AS area_intersection
    FROM {{ ref('usa_cbsa_growth_geom') }} cg
    INNER JOIN county_geom c
    ON cg.geom && c.geom
    AND ST_Intersects(cg.geom, c.geom)
),
crosswalk AS (
    SELECT  cluster_id,
            county_id,
            county_geoid,
            area_county,
            area_intersection,
            area_intersection / NULLIF(area_county, 0) AS area_share_county
    FROM cbsa_county_intersection
)
SELECT *
FROM crosswalk
WHERE area_share_county >= 0.5
