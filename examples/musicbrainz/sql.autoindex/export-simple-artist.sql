-- COPY (SELECT * FROM artist) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        'artist' AS "kind:string:mb",
        a.id AS pk,
        a.gid AS "mbid:string:mbid",
        n.name AS "name:string:mb",
        a.begin_date_year,
        a.end_date_year,
        atype.name AS "type:string:mb",
        area.name AS area,
        g.name AS gender,
        a.comment,
        a.ended
    FROM artist a
    JOIN artist_name n ON a.name=n.id
    LEFT JOIN artist_type atype ON a.type=atype.id
    LEFT JOIN area ON a.area=area.id
    LEFT JOIN gender g ON a.gender=g.id
)
TO stdout CSV HEADER DELIMITER E'\t';
