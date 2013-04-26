-- COPY (SELECT * FROM l_artist_release_group) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        link_arg.id AS pk,
        link_arg.entity0 AS artist_fk,
        link_arg.entity1 AS release_group_fk,
        link_type.name,
        link_type.short_link_phrase,
        link_type.link_phrase,
        link_type.description
    FROM l_artist_release_group link_arg
    JOIN link ON link_arg.link=link.id
    JOIN link_type ON link.link_type=link_type.id
)
TO stdout CSV HEADER DELIMITER E'\t';
