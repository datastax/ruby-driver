# Ruby Driver

Ruby Driver is a ruby client for [Apache Cassandra, a distributed, fault-tolerant and high-throughput nosql database](http://cassandra.apache.org/). It has built-in support for:

* one-off, [prepared](/features/prepared_statements/) and [batch statements](/features/batch_statements/)
* [asynchronous execution](/features/asynchronous_io/)
* automatic peer discovery and cluster metadata
* [diverse load-balancing](/features/load_balancing/), [retry](/features/retry_policies/) and reconnection policies, [with ability to write your own](/features/load_balancing/implementing_a_policy/)

Full list of features can be found in detailed documentation sections on the right.

## Quick Start

### Connecting and Discovering Nodes

```ruby
require 'cql'

cluster = Cql.connect

cluster.hosts.each do |host|
  puts "Host #{host.ip}: id=#{host.id} datacenter=#{host.datacenter} rack=#{host.rack}"
end
```

[Read more in the api docs](/api/#connect-class_method)

### Executing Queries

```ruby
keyspace = 'system'
session  = cluster.connect(keyspace)

session.execute('SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies').each do |row|
  puts "The keyspace #{row['keyspace_name']} has a table called #{row['columnfamily_name']}"
end
```

[Read more in the api docs](/api/session/#execute_async-instance_method)

### Executing Prepared Statements

```ruby
statement = session.prepare('INSERT INTO users (username, email) VALUES (?, ?)')

session.execute(statement, 'avalanche123', 'bulat.shakirzyanov@datastax.com')
```

### Executing Statements in Parallel

```ruby
data = [
  ['username', 'username@example.com'],
  ['another', 'another@example.com']
]

# execute all statements in background
futures = data.map do |(username, email)|
  session.execute_async(statement, username, email)
end

# block until both statements executed
futures.each {|future| future.get}
```

## Installation

As a rubygem:

```console
gem install cassandra-driver
```

Or add the following to your `Gemfile`:

```ruby
gem 'cassandra-driver'
```

## Architecture

The diagram below represents a high level architecture of the driver. Each arrow represents direction of ownership, where owner is pointed to by its children. For example, a single [Cql::Cluster](/api/cluster) instance can manage multiple [Cql::Session](/api/session) instances, etc.

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

### Cluster

A Cluster instance allows to configure different important aspects of the way connections and queries will be handled. At this level you can configure everything from contact points (address of the nodes to be contacted initially before the driver performs node discovery), the request routing policy, retry and reconnection policies, and so forth. Generally such settings are set once at the application level.

```ruby
require 'cql'

cluster = Cql.connect(
            :hosts => ['10.1.1.3', '10.1.1.4', '10.1.1.5'],
            :load_balancing_policy => Cql::LoadBalancing::Policies::DCAwareRoundRobin.new("US_EAST")
          )
```

### Session

Sessions are used for query execution. Internally a Session manages connection pools as well as tracks current keyspace. A session should be reused as much as possible, however it is ok to create several independent session for interacting with different keyspaces in the same application.
