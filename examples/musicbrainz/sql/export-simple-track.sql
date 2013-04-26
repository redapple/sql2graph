-- COPY (SELECT * FROM track) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        t.id AS pk,
        tn.name,
        t.position,
        t.number,
        t.length,
        t.tracklist AS tracklist_fk,
        t.recording AS recording_fk,
        t.artist_credit AS artist_credit_fk
    FROM track t
    JOIN track_name tn ON t.name=tn.id
)
TO stdout CSV HEADER DELIMITER E'\t';
