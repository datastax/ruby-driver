#!/bin/bash

ccm create -n 3 -v git:cassandra-2.0.9 -i 127.0.0. -s -b test-cluster

source ~/env/opt/apache-cassandra-2.0.9/env*.source
echo "CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};" | cqlsh
cqlsh -k simplex -f ../features/support/cql/schema/songs.cql
cqlsh -k simplex -f ../features/support/cql/data/songs.cql

