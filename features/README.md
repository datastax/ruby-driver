# Ruby Driver

Ruby Driver is a ruby client for [Apache Cassandra](http://cassandra.apache.org/), a distributed, fault-tolerant and high-throughput nosql database. It has built-in support for:

* simple queries
* [prepared](/features/prepared_statements/) and [batch statements](/features/batches/)
* [asynchronous execution](/features/asynchronous_io/)
* automatic peer discovery and cluster metadata
* [diverse load-balancing](/features/load_balancing/), [retry](/features/retry_policies/) and reconnection policies, [with ability to write your own](/features/load_balancing/custom/).

Full list of features can be found in detailed documentation sections on the right.

## Quick Start

```ruby
require 'cql'

cluster = Cql.cluster.build
session = cluster.connect(keyspace = 'system')

session.execute('SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies').each do |row|
  puts "The keyspace #{row['keyspace_name']} has a table called #{row['columnfamily_name']}"
end
```

## Installation

As a rubygem:

```console
gem install cql-rb
```

Or add the following to your `Gemfile`:

```ruby
gem 'cql-rb'
```
