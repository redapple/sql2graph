-- COPY (SELECT * FROM release) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        'release' AS "kind:string:mb",
        r.id AS pk,
        r.gid AS "mbid:string:mbid",
        rn.name AS "name:string:mb",
        r.artist_credit AS artist_credit_fk,
        r.release_group AS release_group_fk,
        --r.packaging,
        r.barcode,
        rs.name AS status
    FROM release r
    JOIN release_name rn ON r.name=rn.id
    LEFT JOIN release_status rs ON r.status=rs.id
)
TO stdout CSV HEADER DELIMITER E'\t';
