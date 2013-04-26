-- COPY (SELECT * FROM label) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        l.id AS pk,
        l.gid AS mbid,
        ln.name,
        l.begin_date_year,
        l.end_date_year,
        l.label_code,
        lt.name AS type,
        c.iso_code AS country,
        l.comment,
        l.ended
    FROM label l
    JOIN label_name ln ON l.name = ln.id
    LEFT JOIN label_type lt ON l.type = lt.id
    LEFT JOIN country c ON l.country=c.id
)
TO stdout CSV HEADER DELIMITER E'\t';
