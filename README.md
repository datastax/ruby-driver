# Datastax Ruby Driver for Apache Cassandra

A Ruby client driver for Apache Cassandra. It has built-in support for:

* one-off, [prepared](/features/prepared_statements/) and [batch statements](/features/batch_statements/)
* [asynchronous execution](/features/asynchronous_io/)
* automatic peer discovery and cluster metadata
* [diverse load-balancing](/features/load_balancing/), [retry](/features/retry_policies/) and reconnection policies, [with ability to write your own](/features/load_balancing/implementing_a_policy/)

This driver works exclusively with the Cassandra Query Language v3 (CQL3) and Cassandra's native protocol. Cassandra versions 1.2 and 2.0 are supported as well as Ruby 1.9.3, 2.0, JRuby 1.7 and Rubinius 2.1.

This driver is based on [the cql-rb gem](https://github.com/iconara/cql-rb) by [Theo Hultberg](https://github.com/iconara).

## Quick start

```ruby
require 'cassandra'

cluster = Cassandra.connect # connects to localhost by default

cluster.each_hosts do |host| # automatically discovers all peers
  puts "Host #{host.ip}: id=#{host.id} datacenter=#{host.datacenter} rack=#{host.rack}"
end

keyspace = 'system'
session  = cluster.connect(keyspace) # create session, optionally scoped to a keyspace, to execute queries

future = session.execute_async('SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies') # fully asynchronous api
future.on_success do |rows|
  rows.each do |row|
    puts "The keyspace #{row['keyspace_name']} has a table called #{row['columnfamily_name']}"
  end
end
future.join
```

The host you specify is just a seed node, the driver will automatically discover all peers in the cluster.

Read more:

* [`Cql.connect` options](/api/#connect-class_method)
* [`Session#execute_async` options](/api/session/#execute_async-instance_method)
* [Usage documentation](/features)

## Upgrading from cql-rb

Since Datastax Ruby driver introduces a lot of breaking api changes from cql-rb, it is important to understand what exactly has changed and why:

* [Cassandra.connect](/api/#connect-class_method) returns a [Cassandra::Cluster](/api/cluster/) object that cannot be used to execute queries. A cluster is a metadata container and can be used to get information about all members of cassandra cluster that it is connected to, their respective datacenters, versions and more. [Read about Cassandra::Host](/api/host/) for more info. As well as to register listeners that will be notified whenever membership status changes.
* [Cassandra::Session#execute](/api/session/#execute-instance_method) or [Cassandra::Session#execute_async](/api/session/#execute_async-instance_method) should be used to execute queries and prepare statements.
* [Cluster#connect](/api/cluster/#connect-instance_method) or its asynchronous implementation - [Cluster#connect_async](/api/cluster/#connect_async-instance_method) - are used to get a `Cassandra::Session` instance, optionally scoped to a given `keyspace`.
* [Cassandra::Future](/api/future/) api is incompatible with `Ione::Future`. This is done both to sipmlify the api and to preserve backwards compatibility in the future. Futures interface will stay the same even if Ione implementation changes or if a different reactor altogether is used.
* [Cassandra::Result](/api/result/) has been enhanced to include `#execution_info` and `#next_page_async`. Methods `#trace_id` and `#metadata` have been removed. Trace can be accessed from [Cassandra::Execution::Info](/api/execution/info/) and metadata is considered private.
* [Cassandra::Statements::Prepared](/api/statements/prepared/) now includes `#execution_info`. However `#metadata` and `#result_metadata` have been remove and are not members of the public interface anymore.
* Finally, `#execute` and `#batch` methods have been removed from all statement implementations. [Statements](/api/statements/) are expected to be passed to `Cassandra::Session#execute` for execution.

Below is an example of how to create a thin backwards compatibility shim to ease migration from cql-rb to the ruby driver:

```ruby
require 'cassandra'
require 'ione'

class PreparedStatement
  attr_reader :statement

  def initialize(client, statement)
    @client = client
    @statement = statement
  end

  def execute(*args)
    @client.execute(@statement, *args)
  end
end

class BatchStatement
  def initialize(client, batch)
    @client = client
    @batch = batch
  end

  def execute(options = {})
    @client.execute(@batch, options)
  end

  def add(*args)
    @batch.add(*args)
    self
  end
end

class Client
  def initialize(session)
    @session = session
  end

  def execute(*args)
    future = Ione::CompletableFuture.new
    @session.execute_async(*args).on_complete do |e, v|
      if e
        future.fail(e)
      else
        future.resolve(v)
      end
    end
    future
  end

  def prepare(statement, options = {})
    future = Ione::CompletableFuture.new
    @session.prepare_async(statement, options).on_complete do |e, v|
      if e
        future.fail(e)
      else
        future.resolve(PreparedStatement.new(self, v))
      end
    end
    future
  end

  def batch(type = :logged, options = {})
    batch = BatchStatement.new(self, @session.send(:"#{type}_batch"))
    if block_given?
      yield(batch)
      batch.execute(options)
    else
      batch
    end
  end

  def close
    future = Ione::CompletableFuture.new
    @session.close.on_complete do |e, v|
      if e
        future.fail(e)
      else
        future.resolve(v)
      end
    end
    future
  end
end

cluster = Cassandra.connect
session = cluster.connect
client  = Client.new(session)
```

## Changelog & versioning

Check out the [releases on GitHub](https://github.com/datastax/ruby-driver/releases). Version numbering follows the [semantic versioning](http://semver.org/) scheme.

Private and experimental APIs, defined as whatever is not in the [public API documentation][1], i.e. classes and methods marked as `@private`, will change without warning. If you've been recommended to try an experimental API by the maintainers, please let them know if you depend on that API. Experimental APIs will eventually become public, and knowing how they are used helps in determining their maturity.

Prereleases will be stable, in the sense that they will have finished and properly tested features only, but may introduce APIs that will change before the final release. Please use the prereleases and report bugs, but don't deploy them to production without consulting the maintainers, or doing extensive testing yourself. If you do deploy to production please let the maintainers know as this helps determining the maturity of the release.

## Known bugs & limitations

* JRuby 1.6 is not officially supported, although 1.6.8 should work, if you're stuck in JRuby 1.6.8 try and see if it works for you.
* Large results are buffered in memory until the whole response has been loaded, the protocol makes it possible to start to deliver rows to the client code as soon as the metadata is loaded, but this is not supported yet.

## Copyright

Copyright 2013-2014 DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

  [1]: http://datastax.github.io/ruby-driver/api
