-- COPY (SELECT * FROM l_artist_recording) TO stdout CSV HEADER DELIMITER E'\t';
COPY(
    SELECT
        link_ar.id AS pk,
        link_ar.entity0 AS artist_fk,
        link_ar.entity1 AS recording_fk,
        link_type.name,
        link_type.short_link_phrase,
        link_type.link_phrase,
        link_type.description
    FROM l_artist_recording link_ar
    JOIN link ON link_ar.link=link.id
    JOIN link_type ON link.link_type=link_type.id
)
TO stdout CSV HEADER DELIMITER E'\t';
