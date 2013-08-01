-- COPY (SELECT * FROM l_artist_artist) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        link_aa.id AS pk,
        link_aa.entity0 AS artist1_fk,
        link_aa.entity1 AS artist2_fk,
        link.begin_date_year,
        link.end_date_year,
        link_type.name,
        --link_type.long_link_phrase,
        --link_type.link_phrase,
        link_type.description
    FROM l_artist_artist link_aa
    JOIN link ON link_aa.link=link.id
    JOIN link_type ON link.link_type=link_type.id
)
TO stdout CSV HEADER DELIMITER E'\t';
