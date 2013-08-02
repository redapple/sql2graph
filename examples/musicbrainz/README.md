Importing MusicBrainz data into Neo4J
=====================================

1) Get MusicBrainz data in your database of choice
See http://musicbrainz.org/doc/MusicBrainz_Database/Download

I use Lukas Lalisnky's mbslave https://bitbucket.org/lalinsky/mbslave
(therefore with PostgreSQL)

2) Export MusicBrainz data into simplified table dumps
(mainly resolving name links)

Look into the examples/musicbrainz/sql/ folder to export the data
as TAB delimited CSV files (with 1st row as field names)

You can choose to export only some entities.
E.g. if you're only interested in artists, labels and related URLs,
you could only use entities:
- artist,
- artist_alias,
- label,
- link_artist_artist
- link_artist_label
- link_artist_url
- link_label_label
- link_label_url

Again, I personally use ./mbslave-psql.py

    cat /path/to/examples/musicbrainz/sql/export-simple-artist.sql | ./mbslave-psql.py > dumps/simple-artist.csv
    cat /path/to/examples/musicbrainz/sql/export-simple-label.sql | ./mbslave-psql.py > dumps/simple-label.csv
    ...
    bzip2 dumps/*.csv   # optional

3) Copy mb2neo-simple.conf.default to mb2neo-simple.conf,
  edit it to match where you have the CSV dump files
  and run sql2graph on these dumps files

    python musicbrainz2neo4j.py --config mb2neo-simple.conf --nodes nodes.csv --relations rels.csv

The script needs sql2graph module so you probably need to move what's in examples/musicbrainz/
where appropriate,
or use a symbolic link in the examples/musicbrainz folder
ln -s /path/to/sql2graph/sql2graph sql2graph


4) Import the nodes, relations and indexes files into Neo4J using batch-import

    python /path/to/sql2graph/run_batchimport.py --config mb2neo-simple.conf

**This takes a long time if you import all (or all main) entities.**
