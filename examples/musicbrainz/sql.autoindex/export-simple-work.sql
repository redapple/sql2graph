-- COPY (SELECT * FROM work) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        'work' AS "kind:string:mb",
        w.id AS pk,
        w.gid AS "mbid:string:mbid",
        wname.name AS "name:string:mb",
        wtype.name AS "type:string:mb",
        w.comment
    FROM work w
    JOIN work_name wname ON w.name=wname.id
    LEFT JOIN work_type wtype ON w.type=wtype.id
)
TO stdout CSV HEADER DELIMITER E'\t';
