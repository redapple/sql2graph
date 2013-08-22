COPY(
    SELECT
        link_ee.entity0 AS release_fk,
        link_ee.entity1 AS work_fk,
        link.begin_date_year,
        link.end_date_year,
        link_type.name AS name,
        --link_type.long_link_phrase,
        --link_type.link_phrase,
        link_type.description
    FROM l_release_work link_ee
    JOIN link ON link_ee.link=link.id
    JOIN link_type ON link.link_type=link_type.id
)
TO stdout CSV HEADER DELIMITER E'\t';
