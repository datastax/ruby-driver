# Features

## Basic Usage

### Connecting and Discovering Nodes

```ruby
require 'cassandra'

cluster = Cassandra.connect

cluster.hosts.each do |host|
  puts "Host #{host.ip}: id=#{host.id} datacenter=#{host.datacenter} rack=#{host.rack}"
end
```

[Read more in the api docs](/api/#connect-class_method)

### Executing Queries

You run CQL statements by passing them to `Session#execute`.

```ruby
keyspace = 'system'
session  = cluster.connect(keyspace)

session.execute('SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies').each do |row|
  puts "The keyspace #{row['keyspace_name']} has a table called #{row['columnfamily_name']}"
end
```

[Read more in the api docs](/api/session/#execute-instance_method)

### Parameterized queries 

If you're using Cassandra 2.0 or later you no longer have to build CQL strings when you want to insert a value in a query, there's a new feature that lets you bind values with reqular statements:

```ruby
session.execute("UPDATE users SET age = ? WHERE user_name = ?", 41, 'Sam')
```

If you find yourself doing this often, it's better to use prepared statements. As a rule of thumb, if your application is sending a request more than once, a prepared statement is almost always the right choice.

When you use bound values with regular statements the type of the values has to be guessed. Cassandra supports multiple different numeric types, but there's no reliable way of guessing whether or not a Ruby `Fixnum` should be encoded as a `BIGINT` or `INT`, or whether a Ruby `Float` is a `DOUBLE` or `FLOAT`. When there are multiple choices the encoder will pick the larger type (e.g. `BIGINT` over `INT`). For Ruby strings it will always guess `VARCHAR`, never `BLOB`.

### Executing Statements in Parallel

With fully asynchronous api, it is very easy to run queries in parallel:

```ruby
data = [
  [41, 'Sam'],
  [35, 'Bob']
]

# execute all statements in background
futures = data.map do |(age, username)|
  session.execute_async("UPDATE users SET age = ? WHERE user_name = ?", age, username)
end

# block until both statements executed
futures.each(&:join)
```

[Read more about futures](/api/future/)

### Prepared Statements

The driver supports prepared statements. Use `#prepare` to create a statement object, and then call `#execute` on that object to run a statement. You must supply values for all bound parameters when you call `#execute`.

```ruby
statement = session.prepare('INSERT INTO users (username, email) VALUES (?, ?)')

session.execute(statement, 'avalanche123', 'bulat.shakirzyanov@datastax.com')
```

A prepared statement can be run many times, but the CQL parsing will only be done once on each node. Use prepared statements for queries you run over and over again.

`INSERT`, `UPDATE`, `DELETE` and `SELECT` statements can be prepared, other statements may raise `QueryError`.

For each query, statements are prepared lazily - each call to `#execute` selects a host to try (according to [a load balancing policy](/features/load_balancing/)) and a statement is prepared if needed.

You should only create a prepared statement for any given query once, and then reuse it when calling `#execute`. Preparing the same CQL over and over again is bad for performance since each preparation requires a roundtrip to Cassandra.

### Changing keyspaces

You can specify a keyspace to change to immediately after connection by passing the keyspace option to [`Cql::Cluster#connect`](/api/cluster/#connect-instance_method), you can also use the [`Session#execute`](/api/session/#execute-instance_method) method to change keyspace of an existing session:

```ruby
session.execute('USE measurements')
```

## Creating keyspaces and tables

There is no special facility for creating keyspaces and tables, they are created by executing CQL:

```ruby
keyspace_definition = <<-KEYSPACE_CQL
  CREATE KEYSPACE measurements
  WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
  }
KEYSPACE_CQL

table_definition = <<-TABLE_CQL
  CREATE TABLE events (
    id INT,
    date DATE,
    comment VARCHAR,
    PRIMARY KEY (id)
  )
TABLE_CQL

session.execute(keyspace_definition)
session.execute('USE measurements')
session.execute(table_definition)
```

You can also `ALTER` keyspaces and tables, and you can read more about that in the [CQL3 syntax documentation][1].

### Batching

If you're using Cassandra 2.0 or later you can build batch requests, either from simple or prepared statements. Batches must not contain any select statements, only `INSERT`, `UPDATE` and `DELETE` statements are allowed.

There are a few different ways to work with batches, one is where you build up a batch with a block:

```ruby
batch = session.batch do |batch|
  batch.add("UPDATE users SET name = 'Sue' WHERE user_id = 'unicorn31'")
  batch.add("UPDATE users SET name = 'Kim' WHERE user_id = 'dudezor13'")
  batch.add("UPDATE users SET name = 'Jim' WHERE user_id = 'kittenz98'")
end

session.execute(batch)
```

Another is by creating a batch building it later:

```ruby
batch = session.batch

batch.add("UPDATE users SET name = 'Sue' WHERE user_id = 'unicorn31'")
batch.add("UPDATE users SET name = 'Kim' WHERE user_id = 'dudezor13'")
batch.add("UPDATE users SET name = 'Jim' WHERE user_id = 'kittenz98'")

session.execute(batch)
```

You can mix any combination of statements in a batch:

```ruby
prepared_statement = session.prepare("UPDATE users SET name = ? WHERE user_id = ?")

batch = session.batch do |batch|
  batch.add(prepared_statement, 'Sue', 'unicorn31')
  batch.add("UPDATE users SET age = 19 WHERE user_id = 'unicorn31'")
  batch.add("INSERT INTO activity (user_id, what, when) VALUES (?, 'login', NOW())", 'unicorn31')
end

session.execute(batch)
```

Batches can have one of three different types: `logged`, `unlogged` or `counter`, where `logged` is the default. Their exact semantics are defined in the [Cassandra documentation][2], but this is how you specify which one you want:

```ruby
counter_statement = session.prepare("UPDATE my_counter_table SET my_counter = my_counter + ? WHERE id = ?")

batch = session.counter_batch do |batch|
  batch.add(counter_statement, 3, 'some_counter')
  batch.add(counter_statement, 2, 'another_counter')
end

session.execute(batch)
```

[Read more about `Session#batch`](/api/session/#batch-instance_method)

Cassandra 1.2 also supported batching, but only as a CQL feature, you had to build the batch as a string, and it didn't really play well with prepared statements.

### Paging

If you're using Cassandra 2.0 or later you can page your query results by adding the `:page_size` option to a query:

```ruby
result = client.execute("SELECT * FROM large_table WHERE id = 'partition_with_lots_of_data'", page_size: 100)

while result
  result.each do |row|
    p row
  end
  result = result.next_page
end
```

[Read more about paging](/api/result/#next_page-instance_method)

### Consistency

You can specify the default consistency to use when you create a new `Cluster`:

```ruby
client = Cassandra.connect(consistency: :all)
```

[Read more about default consistency](/api/#connect-class_method)

Consistency can also be passed to `Session#execute` and `Session#execute_async`

```ruby
session.execute('SELECT * FROM users', consistency: :local_quorum)

statement = session.prepare('SELECT * FROM users')
session.execute(statement, consistency: :one)

batch = session.batch
batch.add("UPDATE users SET email = 'sue@foobar.com' WHERE id = 'sue'")
batch.add("UPDATE users SET email = 'tom@foobar.com' WHERE id = 'tom'")
session.execute(batch, consistency: :all)
```

[Read more about `Session#execute`](/api/session/#execute-instance_method)
[Read more about possible consistencies](/api/#CONSISTENCIES-constant)

The default consistency level unless you've set it yourself is `:one`.

Consistency is ignored for `USE`, `TRUNCATE`, `CREATE` and `ALTER` statements, and some (like `:any`) aren't allowed in all situations.

### Compression

The CQL protocol supports frame compression, which can give you a performance boost if your requests or responses are big. To enable it you can specify compression to use in `Cassandra.connect`.

Cassandra currently supports two compression algorithms: Snappy and LZ4. ruby driver supports both, but in order to use them you will have to install the [snappy](http://rubygems.org/gems/snappy) or [lz4-ruby](http://rubygems.org/gems/lz4-ruby) gems separately. Once it's installed you can enable compression like this:

```ruby
cluster = Cassandra.connect(compressor: :snappy)
```

or

```ruby
cluster = Cassandra.connect(compressor: :lz4)
```

Which one should you choose? On paper the LZ4 algorithm is more efficient and the one Cassandra defaults to for SSTable compression. They both achieve roughly the same compression ratio, but LZ4 does it quicker.

### Logging

You can pass a standard Ruby logger to the client to get some more information about what is going on:

```ruby
require 'logger'

cluster = Cassandra.connect(logger: Logger.new($stderr))
```

Most of the logging will be when the driver connects and discovers new nodes, when connections fail and so on. The logging is designed to not cause much overhead and only relatively rare events are logged (e.g. normal requests are not logged).

## Architecture

The diagram below represents a high level architecture of the driver. Each arrow represents direction of ownership, where owner is pointed to by its children. For example, a single [Cassandra::Cluster](/api/cluster) instance can manage multiple [Cassandra::Session](/api/session) instances, etc.

```ditaa
                                  /-------+
                                  |Cluster|<----------------------------------+
                                  +-------/                                   |
                                      ^                                       |
                                      |                                       |
            +-------------------------+-------------------------+             |
            :                         :                         :             |
        /---+---+                 /---+---+                 /---+---+         |
        |Session|                 |Session|                 |Session|         |
        +-------/                 +-------/                 +-------/         |
            ^                         ^                         ^             |
            |                         |                         |             |
     +------+-----+            +------+-----+            +------+-----+       |
     :            :            :            :            :            :       |
/----+-----+ /----+-----+ /----+-----+ /----+-----+ /----+-----+ /----+-----+ |
|Connection| |Connection| |Connection| |Connection| |Connection| |Connection| |
+----+-----/ +----+-----/ +----+-----/ +----+-----/ +----+-----/ +----+-----/ |
     :            :            :            :            :            :       |
     +------------+-=----------+------+-----+-=----------+------------+       |
                                      |                                       |
                                      v                                       |
                                /----------+                                  |
                                |IO Reactor|                                  |
                                +-----+----/                                  |
                                      :                                       |
                                      +---------------------------------------+
```

### Thread safety

Except for results everything in the driver is thread safe. You only need a single cluster object in your application and usually a single session.

Result objects are wrappers around an array of rows and their primary use case is iteration, something that makes little sense to do concurrently. Because of this they've been designed to not be thread safe to avoid the unnecessary cost of locking.

### Cluster

A Cluster instance allows to configure different important aspects of the way connections and queries will be handled. At this level you can configure everything from contact points (address of the nodes to be contacted initially before the driver performs node discovery), the request routing policy, retry and reconnection policies, and so forth. Generally such settings are set once at the application level.

```ruby
require 'cassandra'

cluster = Cassandra.connect(
            :hosts => ['10.1.1.3', '10.1.1.4', '10.1.1.5'],
            :load_balancing_policy => Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new("US_EAST")
          )
```

### Session

Sessions are used for query execution. Internally a Session manages connection pools as well as tracks current keyspace. A session should be reused as much as possible, however it is ok to create several independent session for interacting with different keyspaces in the same application.


## CQL3

This is just a driver for the Cassandra native CQL protocol, it doesn't really know anything about CQL. You can run any CQL3 statement and the driver will return whatever Cassandra replies with.

Read more about CQL3 in the [CQL3 syntax documentation][1] and the [Cassandra query documentation][2].

## Troubleshooting

### I get "connection refused" errors

Make sure that the native transport protocol is enabled. If you're running Cassandra 1.2.5 or later the native transport protocol is enabled by default, if you're running an earlier version (but later than 1.2) you must enable it by editing `cassandra.yaml` and setting `start_native_transport` to `true`.

To verify that the native transport protocol is enabled, search your logs for the message "Starting listening for CQL clients" and look at which IP and port it is binding to.

### I get "Deadlock detected" errors

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

Since prepared statements are tied to a particular connection, you'll need to recreate those after forking as well.

If your process does not fork and you still encounter deadlock errors, it might also be a bug. All IO is done is a dedicated thread, and if something happens that makes that thread shut down, Ruby will detect that the locks that the client code is waiting on can't be unlocked.

### I get `QueryError`

All errors that originate on the server side are raised as `QueryError`. If you get one of these the error is in your CQL or on the server side.

### I'm not getting all elements back from my list/set/map

There's a known issue with collections that get too big. The protocol uses a short for the size of collections, but there is no way for Cassandra to stop you from creating a collection bigger than 65536 elements, so when you do the size field overflows with strange results. The data is there, you just can't get it back.

### Authentication doesn't work

Please open an issue. It should be working, but it's hard to set up and write automated tests for, so there may be edge cases that aren't covered. If you're using Cassandra 2.0 or DataStax Enterprise 3.1 or higher and/or are using something other than the built in `PasswordAuthenticator` your setup is theoretically supported, but it's not field tested.

If you are using DataStax Enterprise earlier than 3.1 authentication is unfortunately not supported. Please open an issue and we might be able to get it working, I just need someone who's willing to test it out. DataStax backported the authentication from Cassandra 2.0 into DSE 3.0, even though it only uses Cassandra 1.2. The authentication logic might not be able to handle this and will try to authenticate with DSE using an earlier version of the protocol. In short, DSE before 3.1 uses a non-standard protocol, but it should be possible to get it working. DSE 3.1 and 4.0 have been confirmed to work.

### I get "end of file reached" / I'm connecting to port 9160 and it doesn't work

Port 9160 is the old Thrift interface, the binary protocol runs on 9042. This is also the default port for ruby-driver, so unless you've changed the port in `cassandra.yaml`, don't override the port.

### Something else is not working

Open an issue and someone will try to help you out. Please include the gem version, Casandra version and Ruby version, and explain as much about what you're doing as you can, preferably the smallest piece of code that reliably triggers the problem. The more information you give, the better the chances you will get help.

## Performance tips

### Use asynchronous apis

To get maximum performance you can't wait for a request to complete before sending the next. Use `_async` method to run multiple requests in parallel

### Use prepared statements

When you use prepared statements you don't have to smash strings together to create a chunk of CQL to send to the server. Avoiding creating many and large strings in Ruby can be a performance gain in itself. Not sending the query every time, but only the actual data also decreases the traffic over the network, and it decreases the time it takes for the server to handle the request since it doesn't have to parse CQL. Prepared statements are also very convenient, so there is really no reason not to use them.

### Use JRuby

If you want to be serious about Ruby performance you have to use JRuby. The ruby driver is completely thread safe, and the CQL protocol is pipelined by design so you can spin up as many threads as you like and your requests per second will scale more or less linearly (up to what your cores, network and Cassandra cluster can deliver, obviously).

Applications using ruby driver and JRuby can do over 10,000 write requests per second from a single EC2 m1.large if tuned correctly.

### Try batching

Batching in Cassandra isn't always as good as in other (non-distributed) databases. Since rows are distributed accross the cluster the coordinator node must still send the individual pieces of a batch to other nodes, and you could have done that yourself instead.

For Cassandra 1.2 it is often best not to use batching at all, you'll have to smash strings together to create the batch statements, and that will waste time on the client side, will take longer to push over the network, and will take longer to parse and process on the server side. Prepared statements are almost always a better choice.

Cassandra 2.0 introduced a new form of batches where you can send a batch of prepared statement executions as one request (you can send non-prepared statements too, but we're talking performance here). These bring the best of both worlds and can be beneficial for some use cases. Some of the same caveats still apply though and you should test it for your use case.

Whenever you use batching, try compression too.

### Try compression

If your requests or responses are big, compression can help decrease the amound of traffic over the network, which is often a good thing. If your requests and responses are small, compression often doesn't do anything. You should benchmark and see what works for you. The Snappy compressor that comes with ruby driver uses very little CPU, so most of the time it doesn't hurt to leave it on.

In read-heavy applications requests are often small, and need no compression, but responses can be big. In these situations you can modify the compressor used to turn off compression for requests completely. The Snappy compressor that comes with ruby driver will not compress frames less than 64 bytes, for example, and you can change this threshold when you create the compressor.

Compression works best for large requests, so if you use batching you should benchmark if compression gives you a speed boost.

  [1]: https://github.com/apache/cassandra/blob/cassandra-2.0/doc/cql3/CQL.textile
  [2]: http://www.datastax.com/documentation/cql/3.1/webhelp/index.html
