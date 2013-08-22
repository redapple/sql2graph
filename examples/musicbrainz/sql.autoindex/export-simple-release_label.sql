-- COPY (SELECT * FROM release_label) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        rl.id AS pk,
        rl.release AS release_fk,
        rl.label AS label_fk,
        rl.catalog_number
    FROM release_label rl
)
TO stdout CSV HEADER DELIMITER E'\t';
