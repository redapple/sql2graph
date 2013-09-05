pablito@ks355836:~/musicbrainz/mbslave$ psql -U sgadmin -h localhost sogigs
Mot de passe pour l'utilisateur sgadmin : 
psql (9.1.9)
Connexion SSL (chiffrement : DHE-RSA-AES256-SHA, bits : 256)
Saisissez « help » pour l'aide.

sogigs=# CREATE EXTENSION btree_gist 
sogigs-# ;
CREATE EXTENSION
sogigs=# \q

