COPY (
    SELECT
        acn.artist_credit AS artist_credit_fk,
        acn.position,
        acn.artist AS artist_fk,
        an.name,
        acn.join_phrase
    FROM artist_credit_name acn
    JOIN artist_name an ON acn.name = an.id
) TO stdout CSV HEADER DELIMITER E'\t';
