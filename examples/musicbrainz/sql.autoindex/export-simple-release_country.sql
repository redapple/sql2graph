-- COPY (SELECT * FROM release_country) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        rc.release AS release_fk,
        rc.country AS area_fk,
        rc.date_year,
        rc.date_month,
        rc.date_day
    FROM release_country rc
    JOIN area a ON rc.country=a.id
)
TO stdout CSV HEADER DELIMITER E'\t';
