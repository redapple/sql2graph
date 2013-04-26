-- COPY (SELECT * FROM tracklist) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        tl.id AS pk
    FROM tracklist tl
)
TO stdout CSV HEADER DELIMITER E'\t';
