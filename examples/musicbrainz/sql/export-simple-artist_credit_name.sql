COPY (
    SELECT
        a.artist_credit AS artist_credit_fk,
        a.position,
        a.artist AS artist_fk,
        n.name,
        a.join_phrase
    FROM artist_credit_name a
    JOIN artist_name n ON a.name = n.id
) TO stdout CSV HEADER DELIMITER E'\t';
