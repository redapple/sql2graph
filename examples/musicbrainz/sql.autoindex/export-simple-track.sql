-- COPY (SELECT * FROM track) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        'track' AS "kind:string:mb",
        t.id AS "pk:int",
        t.gid AS "mbid:string:mbid",
        tn.name AS "name:string:mb",
        t.position AS "position:int",
        t.number AS "number:int",
        t.length AS "lenght:int",
        t.recording AS "recording_fk:int",
        t.medium AS "medium_fk:int",
        t.artist_credit AS "artist_credit_fk:int"
    FROM track t
    JOIN track_name tn ON t.name=tn.id
)
TO stdout CSV HEADER DELIMITER E'\t';
