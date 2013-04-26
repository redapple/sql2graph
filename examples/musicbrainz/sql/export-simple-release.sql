-- COPY (SELECT * FROM release) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        r.id AS pk,
        r.gid AS mbid,
        rn.name,
        r.artist_credit AS artist_credit_fk,
        r.release_group AS release_group_fk,
        rs.name AS status,
        --r.packaging,
        c.iso_code AS country,
        r.date_year,
        r.date_month,
        r.date_day
    FROM release r
    JOIN release_name rn ON r.name=rn.id
    LEFT JOIN release_status rs ON r.status=rs.id
    LEFT JOIN country c ON r.country=c.id
)
TO stdout CSV HEADER DELIMITER E'\t';
