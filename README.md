# Ruby CQL3 driver

_There has not yet been a stable release of this project._


# Requirements

Cassandra 1.2 with the native transport protocol turned on and a modern Ruby. Tested with Ruby 1.9.3 and JRuby and 1.7.x.

# Installation

    gem install --prerelease cql-rb

## Configure Cassandra

The native transport protocol (sometimes called binary protocol, or CQL protocol) is not on by default in Cassandra 1.2, to enable it edit the `cassandra.yaml` file on all nodes in your cluster and set `start_native_transport` to `true`. You need to restart the nodes for this to have effect.

# Quick start

    require 'cql'

    client = Cql::Client.new(host: 'cassandra.example.com')
    client.start!
    client.use('system')
    rows = client.execute('SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies')
    rows.each do |row|
      puts "The keyspace #{row['keyspace_name']} has a table called #{row['columnfamily_name']}""
    end

when you're done you can call `#shutdown!` to disconnect from Cassandra. You can connect to multiple Cassandra nodes by passing multiple comma separated host names to the `:host` option.

## Changing keyspaces

    client.use('measurements')

or using CQL:

    client.execute('USE measurements')

## Running queries

You run CQL statements by passing them to `#execute`. Most statements don't have any result and the call will return nil.

    client.execute("INSERT INTO events (id, date, description) VALUES (23462, '2013-02-24T10:14:23+0000', 'Rang bell, ate food')")

    client.execute("UPDATE events SET description = 'Oh, my' WHERE id = 13126")


If the CQL statement passed to `#execute` returns a result (e.g. it's a `SELECT` statement) the call returns an enumerable of rows:

    rows = client.execute('SELECT date, description FROM events')
    rows.each do |row|
      row.each do |key, value|
        puts "#{key} = #{value}"
      end
    end

The enumerable also has an accessor called `metadata` which returns a description of the rows and columns:

    rows = client.execute('SELECT date, description FROM events'
    rows.metadata['date'].type # => :date

## Creating keyspaces and tables

There is no special facility for creating keyspaces and tables, they are created by executing CQL:

    keyspace_definition = <<-KSDEF
      CREATE KEYSPACE measurements
      WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 3
      }
    KSDEF

    table_definition = <<- TABLEDEF
      CREATE TABLE events (
        id INT,
        date DATE,
        comment VARCHAR,
        PRIMARY KEY (id)
      )
    TABLEDEF

    client.execute(keyspace_definition)
    client.use(measurements)
    client.execute(table_definition)

You can also `ALTER` keyspaces and tables.

## Prepared statements

The driver supports prepared statements. Use `#prepare` to create a statement object, and then call `#execute` on that object to run a statement. You must supply values for all bound parameters when you call `#execute`.

    statement = client.prepare('SELECT date, description FROM events WHERE id = ?')
    rows = statement.execute(1235)

A prepared statement can be run many times, but the CQL parsing will only be done once. Use prepared statements for queries you run over and over again.

`INSERT`, `UPDATE`, `DELETE` and `SELECT` statements can be prepared, other statements may raise `QueryError`.

At this time prepared statements are local to a single connection. Even if you connect to multiple nodes a prepared statement is only ever going to be executed against one of the nodes.

# Consistency levels

The `#execute` method supports setting the desired consistency level for the statement:

    client.execute('SELECT * FROM peers', :local_quorum)

The possible values are: 

* `:any`
* `:one`
* `:two`
* `:three`
* `:quorum`
* `:all`
* `:local_quorum`
* `:each_quorum`

Consistency level is ignored for `USE`, `TRUNCATE`, `CREATE` and `ALTER` statements, and some (like `:any`) aren't allowed in all situations.

## CQL3

This is just a driver for the Cassandra native CQL protocol, it doesn't really know anything about CQL. You can run any CQL3 statement and the driver will return whatever Cassandra replies with.

Read more about CQL3 in the [CQL3 syntax documentation](https://github.com/apache/cassandra/blob/cassandra-1.2/doc/cql3/CQL.textile) and the [Cassandra query documentation](http://www.datastax.com/docs/1.2/cql_cli/querying_cql).

# Known bugs & limitations

* If any connection raises an error the whole IO reactor shuts down.
* JRuby 1.6.8 is not supported, although it should be. The only known issue is that connection failures aren't handled gracefully.
* No automatic peer discovery.
* You can't specify consistency level when executing prepared statements.
* Authentication is not supported.
* Compression is not supported.
* Large results are buffered in memory until the whole response has been loaded, the protocol makes it possible to start to deliver rows to the client code as soon as the metadata is loaded, but this is not supported yet.
* There is no cluster introspection utilities (like the `DESCRIBE` commands in `cqlsh`).

## Copyright

Copyright 2013 Theo Hultberg/Iconara

_Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License You may obtain a copy of the License at_

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

_Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License._