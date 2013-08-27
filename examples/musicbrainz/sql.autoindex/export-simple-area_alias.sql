--COPY (SELECT * FROM area_alias) TO stdout CSV HEADER DELIMITER E'\t';
COPY (
    SELECT
        'area_alias' AS "kind:string:mb",
        aa.id AS pk,
        aa.area AS artist_fk,
        aa.name AS "name:string:mb",
        aa.locale,
        --aa.primary_for_locale,
        aat.name AS "etype:string:mb",
        aa.begin_date_year,
        aa.end_date_year
    FROM area_alias aa
    LEFT JOIN area_alias_type aat ON aa.type=aat.id
)
TO stdout CSV HEADER DELIMITER E'\t';
