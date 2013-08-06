-- COPY (SELECT * FROM url) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        link_au.id AS pk,
        link_au.entity0 AS artist_fk,
        link_au.entity1 AS url_fk,
        link.begin_date_year,
        link.end_date_year,
        link_type.name,
        --link_type.long_link_phrase,
        --link_type.link_phrase,
        link_type.description
    FROM l_artist_url link_au
    JOIN link ON link_au.link=link.id
    JOIN link_type ON link.link_type=link_type.id
)
TO stdout CSV HEADER DELIMITER E'\t';
