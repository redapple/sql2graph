// Countries and their anthem
START a=node:mbid(kind="area") MATCH (a)-[r?:anthem]-(w) WHERE a.typ="Country" RETURN a, a.name, w.name?;

// Countries without an anthem (in MusicBrainz)
START a=node:mb(kind="area")
MATCH (a)-[r?:anthem]-(w)
WHERE a.etype="Country" AND r is NULL
RETURN a, a.name, w.name?;

// alternative
START a=node:mb(kind="area")
WHERE a.etype="Country" AND NOT((a)-[:anthem]-())
RETURN a, a.name;

// alternative with more property filters in START
START a=node:mb("kind:area AND etype:Country")
WHERE NOT((a)-[:anthem]-())
RETURN a, a.name;

// list all Album releases for an artist by name
START mp=node:mb(name="Maxïmo Park")
> MATCH (mp)-[:CREDITED_AS]->(c)-[:CREDITED_ON]->(rg)-[:OF_TYPE]-(o), (rg)<-[:PART_OF]-(r)
> WHERE rg.kind="release_group" AND o.name="Album"
> RETURN rg.name, count(r);

//even faster is you know the cedit name
START ac=node:mb(name="Maxïmo Park")
MATCH
(ac)-[:CREDITED_ON]->(rg)-[:OF_TYPE]-(o),
(rg)<-[:PART_OF]-(r), (r)-[:HAS_STATUS]->(rs)
WHERE rg.kind="release_group" AND o.name="Album" AND rs.name="Official"
RETURN rg.name, count(r), collect(DISTINCT r.name);

//oder by DESCending number of releases
START ac=node:mb(name="Led Zeppelin")
MATCH
(ac)-[:CREDITED_ON]->(rg)-[:OF_TYPE]-(o),
(rg)<-[:PART_OF]-(r),
(r)-[:HAS_STATUS]->(rs) 
WHERE rg.kind="release_group" 
AND o.name="Album" 
AND rs.name="Official" 
RETURN rg.name, count(r) AS sum 
ORDER BY sum DESC;

// war of first names
START ac=node:mb("name:Bob")
MATCH (ac)-[:CREDITED_ON]->(rg)-[:OF_TYPE]-(o),
(rg)<-[:PART_OF]-(r), (r)-[:HAS_STATUS]->(rs)
WHERE rg.kind="release_group" 
AND o.name="Album" 
AND rs.name="Official" 
RETURN ac.name, count(r) AS sum ORDER BY sum DESC LIMIT 10;

// WHo released the most?
neo4j-sh (0)$ START ac=node:mbid(kind="artist_credit")
MATCH (ac)-[:CREDITED_ON]->(rg)-[:OF_TYPE]-(o), 
(rg)<-[:PART_OF]-(r), 
(r)-[:HAS_STATUS]->(rs)
WHERE rg.kind="release_group" 
AND o.name="Album" 
AND rs.name="Official" 
RETURN ac.name, count(rg) AS sum 
ORDER BY sum DESC 
LIMIT 10;
==> +-----------------------------------+
==> | ac.name                   | sum   |
==> +-----------------------------------+
==> | "Various Artists"         | 91820 |
==> | "Johann Sebastian Bach"   | 1646  |
==> | "Wolfgang Amadeus Mozart" | 1399  |
==> | "Ludwig van Beethoven"    | 1196  |
==> | "Ennio Morricone"         | 686   |
==> | "[unknown]"               | 586   |
==> | "Bob Dylan"               | 545   |
==> | "David Bowie"             | 491   |
==> | "Elvis Presley"           | 476   |
==> | "Antonio Vivaldi"         | 475   |
==> +-----------------------------------+
==> 10 rows
==> 
==> 386287 ms


// Christina Aguilera - b202beb7-99bd-47e7-8b72-195c8d72ebdd
// Britney Spears - 45a663b5-b1cb-4a91-bff6-2bef7bbfdd76
// Katy Pery - 122d63fc-8671-43e4-9752-34e846d62a9c
// Lady Gaga - 650e7db6-b795-4eb5-a702-5ea2fc46c848
// Madonna - 79239441-bfd5-4981-a70c-55c3f15c1287
// Beyoncé - 859d0860-d480-4efd-970c-c05d5f1776b8

neo4j-sh (0)$ START a=node:mbid(mbid="859d0860-d480-4efd-970c-c05d5f1776b8")
> MATCH (a)-[r1:CREDITED_AS]-(ac)-[:CREDITED_ON]-(rec)-[r2]-(w) 
> , (tr)-[:IS_RECORDING]-(rec)                                  
> WHERE rec.kind="recording" and w.kind="work"                  
> RETURN a.name, count(tr) AS nb_tracks,  type(r2), w.name      
> ORDER BY nb_tracks DESC                                       
> LIMIT 10;  
+----------------------------------------------------------------------------+
| a.name    | nb_tracks | type(r2)      | w.name                             |
+----------------------------------------------------------------------------+
| "Beyoncé" | 86        | "PERFORMANCE" | "Telephone"                        |
| "Beyoncé" | 68        | "PERFORMANCE" | "Beautiful Liar"                   |
| "Beyoncé" | 61        | "PERFORMANCE" | "Check on It"                      |
| "Beyoncé" | 41        | "PERFORMANCE" | "'03 Bonnie & Clyde"               |
| "Beyoncé" | 39        | "PERFORMANCE" | "Halo"                             |
| "Beyoncé" | 37        | "PERFORMANCE" | "Baby Boy"                         |
| "Beyoncé" | 30        | "PERFORMANCE" | "Irreplaceable"                    |
| "Beyoncé" | 25        | "PERFORMANCE" | "Broken-Hearted Girl"              |
| "Beyoncé" | 19        | "PERFORMANCE" | "Crazy in Love"                    |
| "Beyoncé" | 19        | "PERFORMANCE" | "Single Ladies (Put a Ring on It)" |
+----------------------------------------------------------------------------+
10 rows
4637 ms


// most prolific writers/composers for some pop artists
START a=node:mbid(mbid="859d0860-d480-4efd-970c-c05d5f1776b8")
MATCH (a)-[r1:CREDITED_AS]-(ac)-[:CREDITED_ON]-(rec)-[r2]-(w), (tr)-[:IS_RECORDING]-(rec)                                  
WHERE rec.kind="recording" and w.kind="work"
RETURN w, count(tr) AS nb_tracks
ORDER BY nb_tracks DESC  
LIMIT 20



START a=node:mbid(mbid="859d0860-d480-4efd-970c-c05d5f1776b8")
MATCH (a)-[r1:CREDITED_AS]-(ac)-[:CREDITED_ON]-(rec)-[r2]-(w), (tr)-[:IS_RECORDING]-(rec)                                  
WHERE rec.kind="recording" and w.kind="work"
WITH a, w, count(tr) AS nb_tracks
ORDER BY nb_tracks DESC
LIMIT 10
MATCH w-[?:WRITER|COMPOSER]-(writer)
WHERE writer<> a
RETURN  w.name, collect(writer.name)


START a=node:mbid(mbid="859d0860-d480-4efd-970c-c05d5f1776b8")
MATCH (a)-[r1:CREDITED_AS]-(ac)-[:CREDITED_ON]-(rec)-[r2]-(w), (tr)-[:IS_RECORDING]-(rec)                                  
WHERE rec.kind="recording" and w.kind="work"
WITH a, w, count(tr) AS nb_tracks
ORDER BY nb_tracks DESC
//LIMIT 10
MATCH p=w-[z:WRITER|COMPOSER]-(writer)
WHERE writer<> a
WITH writer, count(p) AS nb_part
RETURN  writer.name, nb_part
ORDER BY nb_part DESC

//Christina Aguilera: b202beb7-99bd-47e7-8b72-195c8d72ebdd
START a=node:mbid(mbid="b202beb7-99bd-47e7-8b72-195c8d72ebdd")
MATCH (a)-[r1:CREDITED_AS]-(ac)-[:CREDITED_ON]-(rec)-[r2]-(w), (tr)-[:IS_RECORDING]-(rec)                                  
WHERE rec.kind="recording" and w.kind="work"
WITH a, w, count(tr) AS nb_tracks
ORDER BY nb_tracks DESC
LIMIT 50
MATCH p=w-[z:WRITER|COMPOSER]-(writer)
WHERE writer<> a
WITH writer, count(p) AS nb_part
RETURN  writer.name, nb_part
ORDER BY nb_part DESC


//122d63fc-8671-43e4-9752-34e846d62a9c
START a=node:mbid(mbid="122d63fc-8671-43e4-9752-34e846d62a9c")
MATCH (a)-[r1:CREDITED_AS]-(ac)-[:CREDITED_ON]-(rec)-[r2]-(w), (tr)-[:IS_RECORDING]-(rec)                                  
WHERE rec.kind="recording" and w.kind="work"
WITH a, w, count(tr) AS nb_tracks
ORDER BY nb_tracks DESC
//LIMIT 50
MATCH p=w-[z:WRITER|COMPOSER]-(writer)
WHERE writer<> a
WITH writer, count(p) AS nb_part
RETURN  writer.name, nb_part
ORDER BY nb_part DESC

// Lady gaga: 650e7db6-b795-4eb5-a702-5ea2fc46c848
START a=node:mbid(mbid="650e7db6-b795-4eb5-a702-5ea2fc46c848")
MATCH (a)-[r1:CREDITED_AS]-(ac)-[:CREDITED_ON]-(rec)-[r2]-(w), (tr)-[:IS_RECORDING]-(rec)                                  
WHERE rec.kind="recording" and w.kind="work"
WITH a, w, count(tr) AS nb_tracks
ORDER BY nb_tracks DESC
//LIMIT 50
MATCH p=w-[z:WRITER|COMPOSER]-(writer)
WHERE writer<> a
WITH writer, count(p) AS nb_part
RETURN  writer.name, nb_part
ORDER BY nb_part DESC


// Madonna
START a=node:mbid(mbid="79239441-bfd5-4981-a70c-55c3f15c1287")
MATCH (a)-[r1:CREDITED_AS]-(ac)-[:CREDITED_ON]-(rec)-[r2]-(w), (tr)-[:IS_RECORDING]-(rec)                                  
WHERE rec.kind="recording" and w.kind="work"
WITH a, w, count(tr) AS nb_tracks
ORDER BY nb_tracks DESC
//LIMIT 50
MATCH p=w-[z:WRITER|COMPOSER]-(writer)
WHERE writer<> a
WITH writer, count(p) AS nb_part
RETURN  writer.name, nb_part
ORDER BY nb_part DESC

// Britney Spears 45a663b5-b1cb-4a91-bff6-2bef7bbfdd76
START a=node:mbid(mbid="45a663b5-b1cb-4a91-bff6-2bef7bbfdd76")
MATCH (a)-[r1:CREDITED_AS]-(ac)-[:CREDITED_ON]-(rec)-[r2]-(w), (tr)-[:IS_RECORDING]-(rec)                                  
WHERE rec.kind="recording" and w.kind="work"
WITH a, w, count(tr) AS nb_tracks
ORDER BY nb_tracks DESC
//LIMIT 50
MATCH p=w-[z:WRITER|COMPOSER]-(writer)
WHERE writer<> a
WITH writer, count(p) AS nb_part
RETURN  writer.name, nb_part
ORDER BY nb_part DESC