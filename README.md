pixelle-de
==========

Pixelle decisioning engine

Sync:
The sync program syncs ad and campaigns from mongodb to local elasticsearch. In addition to all
the fields in ads and campaign collection on mongodb, the sync program also stores the last updated
timestamp for both ads and campaign collection. 

The sync program run every minute. It queries elasticsearch for the last timestamp available for ads 
and campaign collection and finds all documents on mongodb that were created after that timestamp.
The documents found on mongodb are then indexed on elasticsearch.
sync -url "mongodbUrl"


Deserver: The DE server is the decisioning engine. For more info on DE, please see the wiki.

To run the decision engine, simply start
./deserver

