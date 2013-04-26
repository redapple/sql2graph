COPY(
    SELECT
        link_ll.entity0 AS label1_fk,
        link_ll.entity1 AS label2_fk,
        link_type.name AS name,
        link_type.link_phrase,
    FROM l_label_label link_ll
    JOIN link ON link_ll.link=link.id
    JOIN link_type ON link.link_type=link_type.id
)
TO stdout CSV HEADER DELIMITER E'\t';
