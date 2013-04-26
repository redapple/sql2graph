-- COPY (SELECT * FROM url) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        u.id AS pk,
        u.gid AS mbid,
        u.url
    FROM url u
)
TO stdout CSV HEADER DELIMITER E'\t';
