-- COPY (SELECT * FROM area) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        'area' AS "kind:string:mb",
        a.id AS pk,
        a.gid AS "mbid:string:mbid",
        a.name AS "name:string:mb",
        atype.name AS "etype:string:mb"
    FROM area a
    JOIN area_type atype ON a.type=atype.id
)
TO stdout CSV HEADER DELIMITER E'\t';
