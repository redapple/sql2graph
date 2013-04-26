-- COPY (SELECT * FROM recording) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        r.id as pk,
        r.gid AS mbid,
        tn.name,
        r.artist_credit AS artist_credit_fk,
        r.length
    FROM recording r
    JOIN track_name tn ON r.name=tn.id
)
TO stdout CSV HEADER DELIMITER E'\t';
