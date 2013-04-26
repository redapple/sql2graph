-- COPY (SELECT * FROM release_group) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        rg.id AS pk,
        rg.gid AS mbid,
        rn.name,
        rg.artist_credit AS artist_credit_fk,
        rgpt.name AS type,
        rg.comment
    FROM release_group rg
    JOIN release_name rn ON rg.name=rn.id
    LEFT JOIN release_group_primary_type rgpt ON rg.type=rgpt.id
)
TO stdout CSV HEADER DELIMITER E'\t';
