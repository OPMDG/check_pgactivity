#!/bin/bash

# Usage : dockercheck.sh [VERSION]
# with:
#    VERSION : PostgreSQL major version
# 

if [ "$1" = "" ]; then
	PG_VERSION=9.6
else
	PG_VERSION=$1
fi

CONTAINER=test_check_pgactivity_$PG_VERSION

docker run -d --name $CONTAINER postgres:$PG_VERSION > /dev/null

# load test into the 
docker cp . $CONTAINER:/tmp/

# wait for the database to come up	
sleep 10
	
# launch test
docker exec $CONTAINER bash -x /tmp/test.sh $PG_VERSION
rc=$?

# clean up	
docker rm -f $CONTAINER > /dev/null

exit $rc
