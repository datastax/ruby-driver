# Ruby CQL3 driver

[![Build Status](https://travis-ci.org/iconara/cql-rb.png?branch=master)](https://travis-ci.org/iconara/cql-rb)
[![Coverage Status](https://coveralls.io/repos/iconara/cql-rb/badge.png)](https://coveralls.io/r/iconara/cql-rb)
[![Blog](http://b.repl.ca/v1/blog-cqlrb-ff69b4.png)](http://architecturalatrocities.com/tagged/cqlrb)

_If you're reading this on GitHub, please note that this is the readme for the development version and that some features described here might not yet have been released. You can find the readme for a specific version either through [rubydoc.info](http://rubydoc.info/find/gems?q=cql-rb) or via the release tags ([here is an example](https://github.com/iconara/cql-rb/tree/v1.2.0))._

# Requirements

Cassandra 1.2 or later with the native transport protocol turned on and a modern Ruby. It's [tested continuously using Travis](https://travis-ci.org/iconara/cql-rb) with Cassandra 2.0.5, Ruby 1.9.3, 2.0, JRuby 1.7 and Rubinius 2.1.

# Installation

    gem install cql-rb

if you want to use compression you should also install the [snappy gem](http://rubygems.org/gems/snappy):

    gem install snappy

# Quick start

```ruby
require 'cql'

client = Cql::Client.connect(hosts: ['cassandra.example.com'])
client.use('system')
rows = client.execute('SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies')
rows.each do |row|
  puts "The keyspace #{row['keyspace_name']} has a table called #{row['columnfamily_name']}"
end
```

The host you specify is just a seed node, the client will automatically connect to all other nodes in the cluster (or nodes in the same data center if you're running multiple rings).

When you're done you can call `#close` to disconnect from Cassandra:

```ruby
client.close
```

# Usage

The full [API documentation][1] is available from [rubydoc.info](http://rubydoc.info/).

## Changing keyspaces

You can specify a keyspace to change to immediately after connection by passing the `:keyspace` option to `Client.connect`, but you can also use the `#use` method, or `#execute`:

```ruby
client.use('measurements')
```

or using CQL:

```ruby
client.execute('USE measurements')
```

## Running queries

You run CQL statements by passing them to `#execute`.

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

If you're using Cassandra 2.0 or later you no longer have to build CQL strings when you want to insert a value in a query, there's a new feature that lets you use bound values with reqular statements:

```ruby
client.execute("UPDATE users SET age = ? WHERE user_name = ?", 41, 'Sam')
```

If you find yourself doing this often, it's better to use prepared statements. As a rule of thumb, if your application is sending a request more than once, a prepared statement is almost always the right choice.

When you use bound values with regular statements the type of the values has to be guessed. Cassandra supports multiple different numeric types, but there's no reliable way of guessing whether or not a Ruby `Fixnum` should be encoded as a `BIGINT` or `INT`, or whether a Ruby `Float` is a `DOUBLE` or `FLOAT`. When there are multiple choices the encoder will pick the larger type (e.g. `BIGINT` over `INT`). For Ruby strings it will always guess `VARCHAR`, never `BLOB`.

You can override the guessing by passing type hints as an option, see the [API docs][1] for more information.

Each call to `#execute` selects a random connection to run the query on.

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
client.use('measurements')
client.execute(table_definition)
```

You can also `ALTER` keyspaces and tables, and you can read more about that in the [CQL3 syntax documentation][2].

## Prepared statements

The driver supports prepared statements. Use `#prepare` to create a statement object, and then call `#execute` on that object to run a statement. You must supply values for all bound parameters when you call `#execute`.

```ruby
statement = client.prepare('SELECT date, description FROM events WHERE id = ?')

[123, 234, 345].each do |id|
  rows = statement.execute(id)
  # ...
end
```

A prepared statement can be run many times, but the CQL parsing will only be done once on each node. Use prepared statements for queries you run over and over again.

`INSERT`, `UPDATE`, `DELETE` and `SELECT` statements can be prepared, other statements may raise `QueryError`.

Statements are prepared on all connections and each call to `#execute` selects a random connection to run the query on.

You should only create a prepared statement for a query once, and then reuse the prepared statement object. Preparing the same CQL over and over again is bad for performance since each preparation requires a roundtrip to _all_ connected Cassandra nodes.

## Batching

If you're using Cassandra 2.0 or later you can build batch requests, either from regular queries or from prepared statements. Batches can consist of `INSERT`, `UPDATE` and `DELETE` statements.

There are a few different ways to work with batches, one is with a block where you build up a batch that is sent when the block ends:

```ruby
client.batch do |batch|
  batch.add("UPDATE users SET name = 'Sue' WHERE user_id = 'unicorn31'")
  batch.add("UPDATE users SET name = 'Kim' WHERE user_id = 'dudezor13'")
  batch.add("UPDATE users SET name = 'Jim' WHERE user_id = 'kittenz98'")
end
```

Another is by creating a batch and sending it yourself:

```ruby
batch = client.batch
batch.add("UPDATE users SET name = 'Sue' WHERE user_id = 'unicorn31'")
batch.add("UPDATE users SET name = 'Kim' WHERE user_id = 'dudezor13'")
batch.add("UPDATE users SET name = 'Jim' WHERE user_id = 'kittenz98'")
batch.execute
```

You can mix any combination of statements in a batch:

```ruby
prepared_statement = client.prepare("UPDATE users SET name = ? WHERE user_id = ?")
client.batch do |batch|
  batch.add(prepared_statement, 'Sue', 'unicorn31')
  batch.add("UPDATE users SET age = 19 WHERE user_id = 'unicorn31'")
  batch.add("INSERT INTO activity (user_id, what, when) VALUES (?, 'login', NOW())", 'unicorn31')
end
```

Batches can have one of three different types: `logged`, `unlogged` or `counter`, where `logged` is the default. Their exact semantics are defined in the [Cassandra documentation][3], but this is how you specify which one you want:

```ruby
counter_statement = client.prepare("UPDATE my_counter_table SET my_counter = my_counter + ? WHERE id = ?")
client.batch(:counter) do |batch|
  batch.add(counter_statement, 3, 'some_counter')
  batch.add(counter_statement, 2, 'another_counter')
end
```

If you want to execute the same prepared statement multiple times in a batch there is a special variant of the batching feature available from `PreparedStatement`:

```ruby
# the same counter_statement as in the example above
counter_statement.batch do |batch|
  batch.add(3, 'some_counter')
  batch.add(2, 'another_counter')
end
```

Cassandra 1.2 also supported batching, but only as a CQL feature, you had to build the batch as a string, and it didn't really play well with prepared statements.

## Paging

If you're using Cassandra 2.0 or later you can page your query results by adding the `:page_size` option to a query:

```ruby
result_page = client.execute("SELECT * FROM large_table WHERE id = 'partition_with_lots_of_data'", page_size: 100)

while result_page
  result_page.each do |row|
    p row
  end
  result_page = result_page.next_page
end
```

## Consistency

You can specify the default consistency to use when you create a new `Client`:

```ruby
client = Cql::Client.connect(hosts: %w[localhost], default_consistency: :all)
```

The `#execute` (of `Client`, `PreparedStatement` and `Batch`) method also supports setting the desired consistency level on a per-request basis:

```ruby
client.execute('SELECT * FROM users', consistency: :local_quorum)

statement = client.prepared('SELECT * FROM users')
statement.execute(consistency: :one)

batch = client.batch
batch.add("UPDATE users SET email = 'sue@foobar.com' WHERE id = 'sue'")
batch.add("UPDATE users SET email = 'tom@foobar.com' WHERE id = 'tom'")
batch.execute(consistency: :all)

batch = client.batch(consistency: :quorum) do |batch|
  batch.add("UPDATE users SET email = 'sue@foobar.com' WHERE id = 'sue'")
  batch.add("UPDATE users SET email = 'tom@foobar.com' WHERE id = 'tom'")
end
```

For batches the options given to `#execute` take precedence over options given to `#batch`.

The possible values for consistency are:

* `:any`
* `:one`
* `:two`
* `:three`
* `:quorum`
* `:all`
* `:local_quorum`
* `:each_quorum`
* `:local_one`

The default consistency level unless you've set it yourself is `:quorum`.

Consistency is ignored for `USE`, `TRUNCATE`, `CREATE` and `ALTER` statements, and some (like `:any`) aren't allowed in all situations.

## Compression

The CQL protocol supports frame compression, which can give you a performance boost if your requests or responses are big. To enable it you can pass a compressor object when you connect.

Cassandra currently supports two compression algorithms: Snappy and LZ4. Support for Snappy compression ships with cql-rb, but in order to use it you will have to install the [snappy](http://rubygems.org/gems/snappy) gem separately. Once it's installed you can enable compression like this:

```ruby
require 'cql/compression/snappy_compressor'

compressor = Cql::Compression::SnappyCompressor.new
client = Cql::Client.connect(hosts: %w[localhost], compressor: compressor)
```

## Logging

You can pass a standard Ruby logger to the client to get some more information about what is going on:

```ruby
require 'logger'

client = Cql::Client.connect(logger: Logger.new($stderr))
```

Most of the logging will be when the driver connects and discovers new nodes, when connections fail and so on, but also when statements are prepared. The logging is designed to not cause much overhead and only relatively rare events are logged (e.g. normal requests are not logged).

## Tracing

You can request that Cassandra traces a request and records what each node had to do to process the request. To request that a query is traced you can specify the `:trace` option to `#execute`. The request will proceed as normal, but you will also get a trace ID back in your response. This ID can then be used to load up the trace data:

```ruby
result = client.execute("SELECT * FROM users", trace: true)
session_result = client.execute("SELECT * FROM system_traces.sessions WHERE session_id = ?", result.trace_id, consistency: :one)
events_result = client.execute("SELECT * FROM system_traces.events WHERE session_id = ?", result.trace_id, consistency: :one)
```

Notice how you can query tables in other keyspaces by prefixing their names with the keyspace name.

The `system_traces.sessions` table contains information about the request itself; which node was the coordinator, the CQL, the total duration, etc. (if the `duration` column is `null` the trace hasn't been completely written yet and you should load it again later). The `events` table contains information about what happened on each node and at what time. Note that each event only contains the number of seconds that elapsed from when the node started processing the request – you can't easily sort these events in a global order.

## Thread safety

Except for results and batches everything in cql-rb is thread safe. You only need a single client object in your application, in fact creating more than one is a bad idea. Similarily prepared statements are thread safe and should be shared.

There are two things that you should be aware are not thread safe: result objects and batches. Result objects are wrappers around an array of rows and their primary use case is iteration, something that makes little sense to do concurrently. Because of this they've been designed to not be thread safe to avoid the unnecessary cost of locking. Similarily it creating batches aren't usually built concurrently, so to avoid the cost of locking they are not thread safe. If you, for some reason, need to use results or batches concurrently, you're responsible for locking around them. If you do this, you're probably doing something wrong, though.

# CQL3

This is just a driver for the Cassandra native CQL protocol, it doesn't really know anything about CQL. You can run any CQL3 statement and the driver will return whatever Cassandra replies with.

Read more about CQL3 in the [CQL3 syntax documentation][2] and the [Cassandra query documentation][3].

# Troubleshooting

## I get "connection refused" errors

Make sure that the native transport protocol is enabled. If you're running Cassandra 1.2.5 or later the native transport protocol is enabled by default, if you're running an earlier version (but later than 1.2) you must enable it by editing `cassandra.yaml` and setting `start_native_transport` to `true`.

To verify that the native transport protocol is enabled, search your logs for the message "Starting listening for CQL clients" and look at which IP and port it is binding to.

## I get "Deadlock detected" errors

This means that the driver's IO reactor has crashed hard. Most of the time it means that you're using a framework, server or runtime that forks and you call `Client.connect` in the parent process. Check the documentation and see if there's any way you can register to run some piece of code in the child process just after a fork, and connect there.

This is how you do it in Resque:

```ruby
Resque.after_fork = proc do
  # connect to Cassandra here
end
```

and this is how you do it in Passenger:

```ruby
PhusionPassenger.on_event(:starting_worker_process) do |forked|
  if forked
    # connect to Cassandra here
  end
end
```

in Unicorn you do it in the config file:

```ruby
after_fork do |server, worker|
  # connect to Cassandra here
end
```

If your process does not fork and you still encounter deadlock errors, it might also be a bug. All IO is done is a dedicated thread, and if something happens that makes that thread shut down, Ruby will detect that the locks that the client code is waiting on can't be unlocked.

## I get "Bad file descriptor"

If you're using cql-rb on Windows there's an [experimental branch with Windows support](https://github.com/iconara/cql-rb/tree/windows_support). The problem is that Windows does not support non blocking reads on IO objects other than sockets, and the fix is very small. Unfortunately I have no way of properly testing things in Windows, so therefore the "experimental" label.

## I get `QueryError`

All errors that originate on the server side are raised as `QueryError`. If you get one of these the error is in your CQL or on the server side.

## I'm not getting all elements back from my list/set/map

There's a known issue with collections that get too big. The protocol uses a short for the size of collections, but there is no way for Cassandra to stop you from creating a collection bigger than 65536 elements, so when you do the size field overflows with strange results. The data is there, you just can't get it back.

## Authentication doesn't work

Please open an issue. It should be working, but it's hard to set up and write automated tests for, so there may be edge cases that aren't covered. If you're using Cassandra 2.0 or DataStax Enterprise 3.1 or higher and/or are using something other than the built in `PasswordAuthenticator` your setup is theoretically supported, but it's not field tested.

If you are using DataStax Enterprise earlier than 3.1 authentication is unfortunately not supported. Please open an issue and we might be able to get it working, I just need someone who's willing to test it out. DataStax backported the authentication from Cassandra 2.0 into DSE 3.0, even though it only uses Cassandra 1.2. The authentication logic might not be able to handle this and will try to authenticate with DSE using an earlier version of the protocol. In short, DSE before 3.1 uses a non-standard protocol, but it should be possible to get it working. DSE 3.1 and 4.0 have been confirmed to work.

## I get "end of file reached" / I'm connecting to port 9160 and it doesn't work

Port 9160 is the old Thrift interface, the binary protocol runs on 9042. This is also the default port for cql-rb, so unless you've changed the port in `cassandra.yaml`, don't override the port.

## Something else is not working

Open an issue and someone will try to help you out. Please include the gem version, Casandra version and Ruby version, and explain as much about what you're doing as you can, preferably the smallest piece of code that reliably triggers the problem. The more information you give, the better the chances you will get help.

# Performance tips

## Use prepared statements

When you use prepared statements you don't have to smash strings together to create a chunk of CQL to send to the server. Avoiding creating many and large strings in Ruby can be a performance gain in itself. Not sending the query every time, but only the actual data also decreases the traffic over the network, and it decreases the time it takes for the server to handle the request since it doesn't have to parse CQL. Prepared statements are also very convenient, so there is really no reason not to use them.

## Use JRuby

If you want to be serious about Ruby performance you have to use JRuby. The cql-rb client is completely thread safe, and the CQL protocol is pipelined by design so you can spin up as many threads as you like and your requests per second will scale more or less linearly (up to what your cores, network and Cassandra cluster can deliver, obviously).

Applications using cql-rb and JRuby can do over 10,000 write requests per second from a single EC2 m1.large if tuned correctly.

## Try batching

Batching in Cassandra isn't always as good as in other (non-distributed) databases. Since rows are distributed accross the cluster the coordinator node must still send the individual pieces of a batch to other nodes, and you could have done that yourself instead.

For Cassandra 1.2 it is often best not to use batching at all, you'll have to smash strings together to create the batch statements, and that will waste time on the client side, will take longer to push over the network, and will take longer to parse and process on the server side. Prepared statements are almost always a better choice.

Cassandra 2.0 introduced a new form of batches where you can send a batch of prepared statement executions as one request (you can send non-prepared statements too, but we're talking performance here). These bring the best of both worlds and can be beneficial for some use cases. Some of the same caveats still apply though and you should test it for your use case.

Whenever you use batching, try compression too.

## Try compression

If your requests or responses are big, compression can help decrease the amound of traffic over the network, which is often a good thing. If your requests and responses are small, compression often doesn't do anything. You should benchmark and see what works for you. The Snappy compressor that comes with cql-rb uses very little CPU, so most of the time it doesn't hurt to leave it on.

In read-heavy applications requests are often small, and need no compression, but responses can be big. In these situations you can modify the compressor used to turn off compression for requests completely. The Snappy compressor that comes with cql-rb will not compress frames less than 64 bytes, for example, and you can change this threshold when you create the compressor.

Compression works best for large requests, so if you use batching you should benchmark if compression gives you a speed boost.

# Try experimental features

To get maximum performance you can't wait for a request to complete before sending the next. At it's core cql-rb embraces this completely and uses non-blocking IO and an asynchronous model for the request processing. The synchronous API that you use is just a thin façade on top that exists for convenience. If you need to scale to thousands of requests per second, have a look at the client code and look at the asynchronous core, it works very much like the public API, _but using it they should be considererd **experimental**_. Experimental in this context does not mean buggy, it is the core of cql-rb after all, but it means that you cannot rely on it being backwards compatible.

# Changelog & versioning

Check out the [releases on GitHub](https://github.com/iconara/cql-rb/releases). Version numbering follows the [semantic versioning](http://semver.org/) scheme.

Private and experimental APIs, defined as whatever is not in the [public API documentation][1], i.e. classes and methods marked as `@private`, will change without warning. If you've been recommended to try an experimental API by the maintainers, please let them know if you depend on that API. Experimental APIs will eventually become public, and knowing how they are used helps in determining their maturity.

Prereleases will be stable, in the sense that they will have finished and properly tested features only, but may introduce APIs that will change before the final release. Please use the prereleases and report bugs, but don't deploy them to production without consulting the maintainers, or doing extensive testing yourself. If you do deploy to production please let the maintainers know as this helps determining the maturity of the release.

# Known bugs & limitations

* JRuby 1.6 is not officially supported, although 1.6.8 should work, if you're stuck in JRuby 1.6.8 try and see if it works for you.
* Windows is not supported (there is experimental support in the [`windows` branch](https://github.com/iconara/cql-rb/tree/windows_support)).
* Large results are buffered in memory until the whole response has been loaded, the protocol makes it possible to start to deliver rows to the client code as soon as the metadata is loaded, but this is not supported yet.
* There is no cluster introspection utilities (like the `DESCRIBE` commands in `cqlsh`) -- but it's not clear whether that will ever be added, it would be useful, but it is also something that another gem could add on top.

Also check out the [issues](https://github.com/iconara/cql-rb/issues) for open bugs.

# How to contribute

[See CONTRIBUTING.md](CONTRIBUTING.md)

# Copyright

Copyright 2013–2014 Theo Hultberg/Iconara and contributors

_Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License You may obtain a copy of the License at_

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

_Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License._

  [1]: http://rubydoc.info/github/iconara/cql-rb/frames
  [2]: https://github.com/apache/cassandra/blob/cassandra-2.0/doc/cql3/CQL.textile
  [3]: http://www.datastax.com/documentation/cql/3.1/webhelp/index.html
