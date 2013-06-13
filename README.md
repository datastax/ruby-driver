# Ruby CQL3 driver

[![Build Status](https://travis-ci.org/iconara/cql-rb.png?branch=master)](https://travis-ci.org/iconara/cql-rb)

# Requirements

Cassandra 1.2 with the native transport protocol turned on and a modern Ruby. Tested with Ruby 1.9.3 and JRuby and 1.7.x.

# Installation

    gem install cql-rb

## Configure Cassandra

If you're running Cassandra 1.2.5 the native transport protocol is enabled by default, if you're running an earlier version (but later than 1.2) you must enable it by editing `cassandra.yaml` and setting `start_native_transport` to `true`.

# Quick start

```ruby
require 'cql'

client = Cql::Client.connect(host: 'cassandra.example.com')
client.use('system')
rows = client.execute('SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies')
rows.each do |row|
  puts "The keyspace #{row['keyspace_name']} has a table called #{row['columnfamily_name']}"
end
```

when you're done you can call `#close` to disconnect from Cassandra. You can connect to multiple Cassandra nodes by passing multiple comma separated host names to the `:host` option.

# Usage

## Changing keyspaces

```ruby
client.use('measurements')
```

or using CQL:

```ruby
client.execute('USE measurements')
```

## Running queries

You run CQL statements by passing them to `#execute`. Most statements don't have any result and the call will return nil.

```ruby
client.execute("INSERT INTO events (id, date, description) VALUES (23462, '2013-02-24T10:14:23+0000', 'Rang bell, ate food')")

client.execute("UPDATE events SET description = 'Oh, my' WHERE id = 13126")
```

If the CQL statement passed to `#execute` returns a result (e.g. it's a `SELECT` statement) the call returns an enumerable of rows:

```ruby
rows = client.execute('SELECT date, description FROM events')
rows.each do |row|
  row.each do |key, value|
    puts "#{key} = #{value}"
  end
end
```

The enumerable also has an accessor called `metadata` which returns a description of the rows and columns:

```ruby
rows = client.execute('SELECT date, description FROM events'
rows.metadata['date'].type # => :date
```

## Creating keyspaces and tables

There is no special facility for creating keyspaces and tables, they are created by executing CQL:

```ruby
keyspace_definition = <<-KSDEF
  CREATE KEYSPACE measurements
  WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
  }
KSDEF

table_definition = <<-TABLEDEF
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
```

You can also `ALTER` keyspaces and tables.

## Prepared statements

The driver supports prepared statements. Use `#prepare` to create a statement object, and then call `#execute` on that object to run a statement. You must supply values for all bound parameters when you call `#execute`.

```ruby
statement = client.prepare('SELECT date, description FROM events WHERE id = ?')
rows = statement.execute(1235)
```

A prepared statement can be run many times, but the CQL parsing will only be done once. Use prepared statements for queries you run over and over again.

`INSERT`, `UPDATE`, `DELETE` and `SELECT` statements can be prepared, other statements may raise `QueryError`.

At this time prepared statements are local to a single connection. Even if you connect to multiple nodes a prepared statement is only ever going to be executed against one of the nodes.

# Consistency levels

The `#execute` (of `Client` and `PreparedStatement`) method supports setting the desired consistency level for the statement:

```ruby
client.execute('SELECT * FROM peers', :local_quorum)
```

The possible values are: 

* `:any`
* `:one`
* `:two`
* `:three`
* `:quorum`
* `:all`
* `:local_quorum`
* `:each_quorum`

The default consistency level is `:quorum`.

Consistency level is ignored for `USE`, `TRUNCATE`, `CREATE` and `ALTER` statements, and some (like `:any`) aren't allowed in all situations.

## CQL3

This is just a driver for the Cassandra native CQL protocol, it doesn't really know anything about CQL. You can run any CQL3 statement and the driver will return whatever Cassandra replies with.

Read more about CQL3 in the [CQL3 syntax documentation](https://github.com/apache/cassandra/blob/cassandra-1.2/doc/cql3/CQL.textile) and the [Cassandra query documentation](http://www.datastax.com/docs/1.2/cql_cli/querying_cql).

# Troubleshooting

## I get "Deadlock detected" errors

This means that the driver's IO reactor has crashed hard. Most of the time it means that you're using a framework, server or runtime that forks and you call `Client.connect` in the parent process. Check the documentation and see if there's any way you can register to run some piece of code in the child process just after a fork.

This is how you do it in Resque:

```ruby
Resque.after_fork = proc do
  # ...
end
```

and this is how you do it in Passenger:

```ruby
PhusionPassenger.on_event(:starting_worker_process) do |forked|
  if forked
    # ...
  end
end
```

in Unicorn you do it in the config file:

```ruby
after_fork do |server, worker|
  # ...
end
```

If your process does not fork and you still encounter deadlock errors, it might also be a bug. All IO is done is a dedicated thread, and if something happens that makes that thread shut down, Ruby will detect that the locks that the client code is waiting on can't be unlocked.

## I'm not getting all elements back from my list/set/map

There's a known issue with collections that get too big. The protocol uses a short for the size of collections, but there is no way for Cassandra to stop you from creating a collection bigger than 65536 elements, so when you do the size field overflows with strange results. The data is there, you just can't get it back.

## The error backtraces are weird

Yeah, sorry. All IO is asynchronous, and when an error is returned from Cassandra the call stack from when the request was issued is gone. `QueryError` has a `#cql` field that contains the CQL for the request that failed, hopefully that gives you enough information to understand where the error originated.

## Authentication doesn't work

Please open an issue. It should be working, but it's hard to write tests for, so there may be edge cases that aren't covered.

## I'm connecting to port 9160 and it doesn't work

Port 9160 is the old Thrift interface, the binary protocol runs on 9042. This is also the default port for cql-rb, so unless you've changed the port in `cassandra.yaml`, don't override the port.

## One of my Cassandra nodes crashed, and my application crashed, isn't Cassandra supposed to be fault tolerant?

Yes it is, and your data is probably safe. cql-rb is just not completely there yet. Ideally it should handle connectivity issues and just talk to the nodes it can talk to and reconnect when things get back to normal. It's on the roadmap.

## Something else is not working

Open an issue and I'll do my best to help you. Please include the gem version, Casandra version and Ruby version, and explain as much about what you're doing as you can, preferably the smallest piece of code that reliably triggers the problem.

# Known bugs & limitations

* No automatic peer discovery.
* No automatic reconnection on connection failures.
* JRuby 1.6.8 and earlier is not supported, although it probably works fine. The only known issue is that connection failures aren't handled gracefully.
* Compression is not supported.
* Large results are buffered in memory until the whole response has been loaded, the protocol makes it possible to start to deliver rows to the client code as soon as the metadata is loaded, but this is not supported yet.
* There is no cluster introspection utilities (like the `DESCRIBE` commands in `cqlsh`).
* No support for request tracing.

## Copyright

Copyright 2013 Theo Hultberg/Iconara

_Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License You may obtain a copy of the License at_

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

_Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License._