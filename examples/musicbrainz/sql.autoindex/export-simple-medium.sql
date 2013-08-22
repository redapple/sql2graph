-- COPY (SELECT * FROM medium) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        'medium' AS "kind:string:mb",
        m.id AS pk,
        m.release AS release_fk,
        m.position,
        mf.name AS format,
        m.name AS "name:string:mb"
    FROM medium m
    LEFT JOIN medium_format mf ON m.format=mf.id
)
TO stdout CSV HEADER DELIMITER E'\t';
