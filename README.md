pixelle-de
==========

Pixelle decisioning engine

go get github.com/dailymotion/pixelle-de/...

Sync: The sync program syncs ad and campaigns from mongodb to local elasticsearch. In addition to all the fields in ads and campaign collection on mongodb, the sync program also stores the last updated timestamp for both ads and campaign collection. The sync program run every minute. It queries elasticsearch for the last timestamp available for ads and campaign collection and finds all documents on mongodb that were created after that timestamp.The documents found on mongodb are then indexed on elasticsearch.

Installation: 
go get github.com/dailymotion/pixelle-de/...

To run the sync process, provide the api url :
Test env:
pxl-desync --api-url=https://api.pxlad.in/adunits -repeat 10 --user=de@pxlad.in --pass=<pass>

Prod env:
pxl-desync --api-url=https://api.pxlad.io/adunits -repeat 60 --user=de@pxlad.io --pass=<pass>

See sync -help for more info.


Deserver: The DE server is the decisioning engine. For more info on DE, please see the wiki.

Installation: 
go get github.com/dailymotion/pixelle-de/...

To run the decision engine, simply start
./deserver [see deserver -help for more info]

