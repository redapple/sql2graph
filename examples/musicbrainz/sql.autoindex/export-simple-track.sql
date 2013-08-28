-- COPY (SELECT * FROM track) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        'track' AS "kind:string:mb",
        t.id AS "pk:int",
        t.gid AS "mbid:string:mbid",
        tn.name AS "name:string:mb",
        t.position AS "position:int",
        t.number,
        t.length AS "length:int",
        t.recording AS "recording_fk:int",
        t.medium AS "medium_fk:int",
        t.artist_credit AS "artist_credit_fk:int"
    FROM track t
    JOIN track_name tn ON t.name=tn.id

    WHERE t.gid != 'f2bf6ffe-1dee-36b9-ba48-1ac1e82e1bc5'
)
TO stdout CSV HEADER DELIMITER E'\t';
