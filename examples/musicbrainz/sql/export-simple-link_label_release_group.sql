COPY(
    SELECT
        lee.id AS pk,
        lee.entity0 AS label_fk,
        lee.entity1 AS release_group_fk,
        link.begin_date_year,
        link.end_date_year,
        link_type.name,
        --link_type.long_link_phrase,
        --link_type.link_phrase,
        link_type.description
    FROM l_label_release_group lee
    JOIN link ON lee.link=link.id
    JOIN link_type ON link.link_type=link_type.id
)
TO stdout CSV HEADER DELIMITER E'\t';
