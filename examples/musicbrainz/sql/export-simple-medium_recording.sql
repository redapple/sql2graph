-- COPY (SELECT * FROM track) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        r.id AS recording_fk,
        m.id AS medium_fk
    FROM tracklist tl
    JOIN medium m ON m.tracklist=tl.id
    JOIN track t ON t.tracklist=tl.id
    JOIN recording r ON t.recording=r.id
)
TO stdout CSV HEADER DELIMITER E'\t';

