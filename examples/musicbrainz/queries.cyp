// Countries and their anthem
MATCH (c:Country)
WITH c
MATCH c-[r?:ANTHEM]-(w:Work)
RETURN c.name, w.name;


// Countries without an anthem (in MusicBrainz)
MATCH (c:Country)
WHERE NOT c-[:ANTHEM]-(:Work)
RETURN c.name;


// list all Album releases for an artist by name
START abba=node:mb_fulltext(name='ABBA')
MATCH (abba:Artist)
WITH abba
MATCH (abba)-[:CREDITED_AS]->()-[:CREDITED_ON]->(rg:ReleaseGroup:Album),
rg-[:PART_OF]-(r:Release:Official)
RETURN rg.name, count(r);

//even faster is you know the cedit name
START abba=node:mb_fulltext(name='ABBA')
MATCH (abba:ArtistCredit)
WITH abba
MATCH abba-[:CREDITED_ON]->(rg:ReleaseGroup:Album),
rg-[:PART_OF]-(r:Release:Official)
RETURN rg.name, count(r);

//oder by DESCending number of releases
START ledzep=node:mb_fulltext(name='Led Zeppelin')
MATCH (ledzep:ArtistCredit)
WITH ledzep
MATCH ledzep-[:CREDITED_ON]->(rg:ReleaseGroup:Album),
rg-[:PART_OF]-(r:Release:Official)
RETURN rg.name, count(r) AS sum
ORDER BY sum DESC;

// war of first names
START ac=node:mb_fulltext("name:Bob")
MATCH (ac:ArtistCredit)
WITH ac
MATCH ac-[:CREDITED_ON]->(rg:ReleaseGroup:Album),
rg-[:PART_OF]-(r:Release:Official)
RETURN ac.name, count(r) AS sum
ORDER BY sum DESC LIMIT 10;
+---------------------------------------------------------+
| ac.name                                           | sum |
+---------------------------------------------------------+
| "Bob Dylan"                                       | 45  |
| "Bob Seger & The Silver Bullet Band"              | 10  |
| "Bob Rivers"                                      | 9   |
| "Bob Mould"                                       | 9   |
| "Bob & Tom"                                       | 8   |
| "bob hund"                                        | 6   |
| "Bob Zany"                                        | 1   |
| "Bob Hansson & Institutet för Höghastighetskonst" | 1   |
| "Bob James"                                       | 1   |
| "Bob Schneider"                                   | 1   |
+---------------------------------------------------------+


// WHo released the most?
MATCH (ac:ArtistCredit)-[:CREDITED_ON]->(rg:ReleaseGroup:Album),
rg-[:PART_OF]-(r:Release:Official)
RETURN ac.name, count(r) AS sum
ORDER BY sum DESC LIMIT 10;

+----------------------------------+
| ac.name                   | sum  |
+----------------------------------+
| "Various Artists"         | 2097 |
| "King Crimson"            | 95   |
| "Jethro Tull"             | 93   |
| "David Bowie"             | 84   |
| "Miles Davis"             | 68   |
| "Uriah Heep"              | 68   |
| "The Legendary Pink Dots" | 64   |
| "The Fall"                | 56   |
| "Joe Jackson"             | 55   |
| "Eric Clapton"            | 54   |
+----------------------------------+

// Christina Aguilera - b202beb7-99bd-47e7-8b72-195c8d72ebdd
// Britney Spears - 45a663b5-b1cb-4a91-bff6-2bef7bbfdd76
// Katy Pery - 122d63fc-8671-43e4-9752-34e846d62a9c
// Lady Gaga - 650e7db6-b795-4eb5-a702-5ea2fc46c848
// Madonna - 79239441-bfd5-4981-a70c-55c3f15c1287
// Beyoncé - 859d0860-d480-4efd-970c-c05d5f1776b8

START a=node:mb_exact(mbid="79239441-bfd5-4981-a70c-55c3f15c1287")
MATCH (a)-[credit:CREDITED_AS]-(ac),
(ac)-[:CREDITED_ON]-(rec:Recording)-[r2]-(w:Work),
(tr:Track)-[:IS_RECORDING]-(rec)
RETURN a.name, count(tr) AS nb_tracks,  type(r2), w.name
ORDER BY nb_tracks DESC
LIMIT 10;


// most prolific writers/composers for some pop artists
// Madonna
START a=node:mb_exact(mbid="45a663b5-b1cb-4a91-bff6-2bef7bbfdd76")
MATCH (a)-[:CREDITED_AS]-(ac),
(ac)-[:CREDITED_ON]-(rec:Recording),
(rec)--(w:Work),
(tr)-[:IS_RECORDING]-(rec)
WITH a, w, count(tr) AS nb_tracks
ORDER BY nb_tracks DESC
MATCH p=w-[z:WRITER|COMPOSER]-(writer)
WHERE writer <> a
WITH writer, count(p) AS nb_part
RETURN  writer.name, nb_part
ORDER BY nb_part DESC;

// most prolific writers/composers alltogether
// this could take long!
MATCH (a:Artist)-[:CREDITED_AS]-(ac),
(ac)-[:CREDITED_ON]-(rec:Recording),
(rec)--(w:Work),
(tr)-[:IS_RECORDING]-(rec)
WITH a, w, count(tr) AS nb_tracks
ORDER BY nb_tracks DESC
MATCH p=w-[z:WRITER|COMPOSER]-(writer)
WHERE writer <> a
WITH writer, count(p) AS nb_part
RETURN  writer.name, nb_part
ORDER BY nb_part DESC;
