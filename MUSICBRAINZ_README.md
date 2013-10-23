# Exporting the Musicbrainz database into Neo4j


    python musicbrainz2neo4j-export.py > musicbrainz2neo4j.sql
    cd /path/to/mbslave
    cat /path/to/musicbrainz2neo4j.sql | ./mbslave-psql.py
    
By default, the generated TSV/CSV files are in /tmp

Now, use the batch-import project to import the csv files into neo4j, using a `mb` and `mbid` index in a custom `./batch.properties` file, putting the database under `./musicbrainz.db`.
    
    cd /path/to/jexp/batch-import
    echo "batch_import.node_index.mbid=exact\nbatch_import.node_index.mb=fulltext" > batch.properties
    MAVEN_OPTS="-server -Xmx10G" && mvn exec:java -Dexec.mainClass="org.neo4j.batchimport.Importer" -Dexec.args="batch.properties muscbrainz.db /tmp/musicbrainz__nodes__full.csv /tmp/musicbrainz__rels__full.csv"