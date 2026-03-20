{{ config(
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
) }}

SELECT  "GISJOIN" AS county_id,
        "GEOID10" AS county_geoid,
        LPAD(CAST("STATEFP10" AS TEXT), 2, '0') AS state_fips,
        ST_Multi(
            ST_CollectionExtract(
                ST_MakeValid(ST_Transform(geometry, 5070)),
                3
            )
        ) AS geom
FROM {{ source('suburbanization', 'usa_county_geom_2010_raw') }}
WHERE geometry IS NOT NULL
AND NOT ST_IsEmpty(geometry)
