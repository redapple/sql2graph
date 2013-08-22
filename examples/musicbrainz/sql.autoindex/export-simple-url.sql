-- COPY (SELECT * FROM url) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        'url' AS "kind:string:mb",
        u.id AS pk,
        u.gid AS "mbid:string:mbid",
        u.url
    FROM url u
)
TO stdout CSV HEADER DELIMITER E'\t';
