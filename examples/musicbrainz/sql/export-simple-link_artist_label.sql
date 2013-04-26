-- COPY (SELECT * FROM l_artist_label) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        link_al.id AS pk,
        link_al.entity0 AS artist_fk,
        link_al.entity1 AS label_fk,
        link_type.name,
        link_type.short_link_phrase,
        link_type.link_phrase,
        link_type.description
    FROM l_artist_label link_al
    JOIN link ON link_al.link=link.id
    JOIN link_type ON link.link_type=link_type.id
)
TO stdout CSV HEADER DELIMITER E'\t';
