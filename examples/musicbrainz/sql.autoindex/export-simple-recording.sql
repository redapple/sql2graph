-- COPY (SELECT * FROM recording) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        'recording' AS "kind:string:mb",
        r.id AS "pk:int",
        r.gid AS "mbid:string:mbid",
        tn.name AS "name:string:mb",
        r.artist_credit AS "artist_credit_fk:int",
        r.length AS "length:int"
    FROM recording r
    JOIN track_name tn ON r.name=tn.id
)
TO stdout CSV HEADER DELIMITER E'\t';
