--COPY (SELECT * FROM artist_alias) TO stdout CSV HEADER DELIMITER E'\t';
COPY (
    SELECT
        'artist_alias' AS "kind:string:mb",
        aa.id AS pk,
        aa.artist AS artist_fk,
        an.name AS "name:string:mb",
        aa.locale,
        --aa.primary_for_locale,
        aat.name AS "etype:string:mb",
        aa.begin_date_year,
        aa.end_date_year
    FROM artist_alias aa
    JOIN artist_name an ON aa.name=an.id
    LEFT JOIN artist_alias_type aat ON aa.type=aat.id
)
TO stdout CSV HEADER DELIMITER E'\t';
