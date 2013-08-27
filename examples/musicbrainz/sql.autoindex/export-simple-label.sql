-- COPY (SELECT * FROM label) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        'label' AS "kind:string:mb",
        l.id AS pk,
        l.gid AS "mbid:string:mbid",
        ln.name AS "name:string:mb",
        l.begin_date_year,
        l.end_date_year,
        l.label_code,
        lt.name AS "etype:string:mb",
        a.name AS area,
        l.comment,
        l.ended
    FROM label l
    JOIN label_name ln ON l.name = ln.id
    LEFT JOIN label_type lt ON l.type = lt.id
    LEFT JOIN area a ON l.area=a.id
)
TO stdout CSV HEADER DELIMITER E'\t';
