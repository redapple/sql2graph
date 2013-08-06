COPY (
    SELECT
        ac.id AS pk,
        an.name
    FROM artist_credit ac
    JOIN artist_name an ON ac.name=an.id
) TO stdout CSV HEADER DELIMITER E'\t';
