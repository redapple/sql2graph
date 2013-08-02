-- COPY (SELECT * FROM release_unknown_country) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        rc.release AS release_fk,
        rc.date_year,
        rc.date_month,
        rc.date_day
    FROM release_country rc
)
TO stdout CSV HEADER DELIMITER E'\t';
