COPY (
    SELECT
        a.id AS pk,
        n.name
    FROM artist_credit a
    JOIN artist_name n ON a.name=n.id
) TO stdout CSV HEADER DELIMITER E'\t';
