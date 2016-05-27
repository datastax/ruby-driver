# Datastax Ruby Driver for Apache Cassandra

*If you're reading this on GitHub, please note that this is the readme for the development version and that some features described here might not yet have been released. You can [find the documentation for latest version through ruby driver docs](http://datastax.github.io/ruby-driver/) or via the release tags, [e.g. v3.0.0](https://github.com/datastax/ruby-driver/tree/v3.0.0).*

[![Build Status](https://travis-ci.org/datastax/ruby-driver.svg?branch=master)](https://travis-ci.org/datastax/ruby-driver)

A Ruby client driver for Apache Cassandra. This driver works exclusively with
the Cassandra Query Language version 3 (CQL3) and Cassandra's native protocol.

- Code: https://github.com/datastax/ruby-driver
- Docs: http://datastax.github.io/ruby-driver/
- Jira: https://datastax-oss.atlassian.net/browse/RUBY
- Mailing List: https://groups.google.com/a/lists.datastax.com/forum/#!forum/ruby-driver-user
- IRC: #datastax-drivers on [irc.freenode.net](http://freenode.net>)
- Twitter: Follow the latest news about DataStax Drivers - [@avalanche123](http://twitter.com/avalanche123), [@stamhankar999](http://twitter.com/stamhankar999), [@al3xandru](https://twitter.com/al3xandru)

This driver is based on [the cql-rb gem](https://github.com/iconara/cql-rb) by [Theo Hultberg](https://github.com/iconara) and we added support for:

* [Asynchronous execution](http://datastax.github.io/ruby-driver/features/asynchronous_io/)
* One-off, [prepared](http://datastax.github.io/ruby-driver/features/basics/prepared_statements/) and [batch statements](http://datastax.github.io/ruby-driver/features/basics/batch_statements/)
* Automatic peer discovery and cluster metadata with [support for change notifications](http://datastax.github.io/ruby-driver/features/state_listeners/)
* Various [load-balancing](http://datastax.github.io/ruby-driver/features/load_balancing/), [retry](http://datastax.github.io/ruby-driver/features/retry_policies/) and [reconnection](http://datastax.github.io/ruby-driver/features/reconnection/) policies with [ability to write your own](http://datastax.github.io/ruby-driver/features/load_balancing/implementing_a_policy/)
* [SSL encryption](http://datastax.github.io/ruby-driver/features/security/ssl_encryption/)
* [Flexible and robust error handling](http://datastax.github.io/ruby-driver/features/error_handling/)
* [Per-request execution information and tracing](http://datastax.github.io/ruby-driver/features/debugging/)
* [Configurable address resolution](http://datastax.github.io/ruby-driver/features/address_resolution/)

[Check out the slides from Ruby Driver Explained](https://speakerdeck.com/avalanche123/ruby-driver-explained) for a detailed overview of the Ruby Driver architecture.

## Compatibility

This driver works exclusively with the Cassandra Query Language v3 (CQL3) and Cassandra's native protocol. The current version works with:

* Apache Cassandra versions 1.2, 2.0, 2.1, 2.2, and 3.x
* DataStax Enterprise 4.0 and above.
* Ruby (MRI) 2.2, 2.3
* JRuby 1.7

__Note__: JRuby 1.6 is not officially supported, although 1.6.8 should work. Similarly,
MRI 2.0 and 2.1 are not officially supported, but they should work. 1.9.3 is deprecated
and is likely to break in the release following 3.0.

## Quick start

```ruby
require 'cassandra'

cluster = Cassandra.cluster # connects to localhost by default

cluster.each_host do |host| # automatically discovers all peers
  puts "Host #{host.ip}: id=#{host.id} datacenter=#{host.datacenter} rack=#{host.rack}"
end

keyspace = 'system'
session  = cluster.connect(keyspace) # create session, optionally scoped to a keyspace, to execute queries

future = session.execute_async('SELECT keyspace_name, table_name FROM system_schema.tables') # fully asynchronous api
future.on_success do |rows|
  rows.each do |row|
    puts "The keyspace #{row['keyspace_name']} has a table called #{row['table_name']}"
  end
end
future.join
```

__Note__: The host you specify is just a seed node, the driver will automatically discover all peers in the cluster.

Read more:

* [`Cassandra.cluster` options](http://datastax.github.io/ruby-driver/api/#cluster-class_method)
* [`Session#execute_async` options](http://datastax.github.io/ruby-driver/api/session/#execute_async-instance_method)
* [Usage documentation](http://datastax.github.io/ruby-driver/features)

## Installation

Install via rubygems

```bash
gem install cassandra-driver
```

Install via Gemfile

```ruby
gem 'cassandra-driver'
```

__Note__: if you want to use compression you should also install [snappy](http://rubygems.org/gems/snappy) or [lz4-ruby](http://rubygems.org/gems/lz4-ruby). [Read more about compression.](http://datastax.github.io/ruby-driver/features/#compression)


## Upgrading from cql-rb

Some of the new features added to the driver have unfortunately led to changes in the original cql-rb API. In the examples directory, you can find [an example of how to wrap the ruby driver to achieve almost complete interface parity with cql-rb](https://github.com/datastax/ruby-driver/blob/master/examples/cql-rb-wrapper.rb) to assist you with gradual upgrade.

## What's new in v3.0.0

### Features:

* Add support for Apache Cassandra native protocol v4
* Add support for smallint, tinyint, date (`Cassandra::Date`) and time (`Cassandra::Time`) data types.
* Include schema metadata for User Defined Functions and User Defined Aggregates.
* Augment the `Cassandra::Table` object to expose many more attributes: `id`, `options`, `keyspace`, `partition_key`, `clustering_columns`, and `clustering_order`. This makes it significantly easier to write administration scripts that report various attributes of your schema, which may help to highlight areas for improvement.
* Include client ip addresses in request traces, only on Cassandra 3.x.
* Add new retry policy decision `Cassandra::Retry::Policy#try_next_host`.
* Support specifying statement idempotence with the new `idempotent` option when executing.
* Support sending custom payloads when preparing or executing statements using the new `payload` option.
* Expose custom payloads received with responses on server exceptions and `Cassandra::Execution::Info` instances.
* Expose server warnings on server exceptions and `Cassandra::Execution::Info` instances.
* Add `connections_per_local_node`, `connections_per_remote_node`, `requests_per_connection` cluster configuration options to tune parallel query execution and resource usage.
* Add `Cassandra::Logger` class to make it easy for users to enable debug logging in the client.
* Add `protocol_version` configuration option to allow the user to force the protocol version to use for communication with nodes.
* Add support for materialized views and indexes in the schema metadata.
* Support the `ReadError`, `WriteError`, and `FunctionCallError` Cassandra error responses introduced in Cassandra 2.2.
* Add support for unset variables in bound statements.
* Support DSE security (`DseAuthenticator`, configured for LDAP).
* Add a timeout option to `Cassandra::Future#get`.

### Breaking Changes from 2.x:

* `Cassandra::Future#join` is now an alias to Cassandra::Future#get and will raise an error if the future is resolved with one.
* Default consistency level is now LOCAL_ONE.
* Enable tcp no-delay by default.
* Unavailable errors are retried on the next host in the load balancing plan by default.
* Statement execution no longer retried on timeouts, unless the statement is marked as idempotent in the call to `Cassandra::Session#execute*` or when creating a `Cassandra::Statement` object. 
* `Cassandra::Statements::Batch#add` and `Cassandra::Session#execute*` signatures have changed in how one specifies query parameters. Specify the query parameters array as the value of the arguments key:
 
```ruby
batch.add(query, ['val1', 'val2'])
# becomes
batch.add(query, arguments: ['val1', 'val2'])

batch.add(query, {p1: 'val1'})
# becomes
batch.add(query, arguments: {p1: 'val1'})
```
* The Datacenter-aware load balancing policy (`Cassandra::LoadBalancing::Policies::DCAwareRoundRobin`) defaults to using
  nodes in the local DC only. In prior releases, the policy would fall back to remote nodes after exhausting local nodes.
  Specify a positive value (or nil for unlimited) for `max_remote_hosts_to_use` when initializing the policy to allow remote node use.
* Unspecified variables in statements previously resulted in an exception. Now they are essentially ignored or treated as null.

### Bug Fixes:

* [[RUBY-120](https://datastax-oss.atlassian.net/browse/RUBY-120)] Tuples and UDTs can be used in sets and hash keys.
* [[RUBY-143](https://datastax-oss.atlassian.net/browse/RUBY-143)] Retry querying system table for metadata of new hosts when prior attempts fail, ultimately enabling use of new hosts.
* [[RUBY-150](https://datastax-oss.atlassian.net/browse/RUBY-150)] Fixed a protocol decoding error that occurred when multiple messages are available in a stream.
* [[RUBY-151](https://datastax-oss.atlassian.net/browse/RUBY-151)] Decode incomplete UDTs properly.
* [[RUBY-155](https://datastax-oss.atlassian.net/browse/RUBY-155)] Request timeout timer should not include request queuing time.
* [[RUBY-161](https://datastax-oss.atlassian.net/browse/RUBY-161)] Protocol version negotiation in mixed version clusters should not fall back to v1 unless it is truly warranted.
* [[RUBY-214](https://datastax-oss.atlassian.net/browse/RUBY-214)] Ensure client timestamps have microsecond precision in JRuby. Previously, some row updates would get lost in high transaction environments.

## Feedback Requested

*Help us focus our efforts!* [Provide your input](http://goo.gl/forms/pCs8PTpHLf) on the Ruby Driver Platform and Runtime Survey (we kept it short).

## Code examples

The DataStax Ruby Driver uses the awesome [Cucumber Framework](http://cukes.info/) for
both end-to-end, or acceptance, testing and constructing documentation. All of the
features supported by the driver have appropriate acceptance tests with easy-to-copy code
examples in the `features/` directory.

## Running tests

If you don't feel like reading through the following instructions on how to run
ruby-driver tests, feel free to [check out .travis.yml for the entire build code](https://github.com/datastax/ruby-driver/blob/master/.travis.yml).

* Check out the driver codebase and install test dependencies:

```bash
git clone https://github.com/datastax/ruby-driver.git
cd ruby-driver
bundle install --without docs
```

* [Install ccm](http://www.datastax.com/dev/blog/ccm-a-development-tool-for-creating-local-cassandra-clusters)

* Run tests against different versions of Cassandra:

```bash
CASSANDRA_VERSION=3.1.1 bundle exec cucumber # runs end-to-end tests (or bundle exec rake cucumber)
CASSANDRA_VERSION=3.0.0 bundle exec rspec # runs unit tests (or bundle exec rake rspec)
CASSANDRA_VERSION=2.1.12 bundle exec rake integration # run integration tests
CASSANDRA_VERSION=2.0.17 bundle exec rake test # run both as well as integration tests
```

## Changelog & versioning

Check out the [releases on GitHub](https://github.com/datastax/ruby-driver/releases) and
[changelog](https://github.com/datastax/ruby-driver/blob/master/CHANGELOG.md). Version
numbering follows the [semantic versioning](http://semver.org/) scheme.

Private and experimental APIs, defined as whatever is not in the
[public API documentation][1], i.e. classes and methods marked as `@private`, will change
without warning. If you've been recommended to try an experimental API by the maintainers,
please let them know if you depend on that API. Experimental APIs will eventually become
public, and knowing how they are used helps in determining their maturity.

Prereleases will be stable, in the sense that they will have finished and properly tested
features only, but may introduce APIs that will change before the final release. Please
use the prereleases and report bugs, but don't deploy them to production without
consulting the maintainers, or doing extensive testing yourself. If you do deploy to
production please let the maintainers know as this helps in determining the maturity of
the release.

## Known bugs & limitations

* Specifying a `protocol_version` option of 1 or 2 in cluster options will fail with a
  `NoHostsAvailable` error rather than a `ProtocolError` against Cassandra node versions 3.0-3.4.
* JRuby 1.6 is not officially supported, although 1.6.8 should work.
* Because the driver reactor is using `IO.select`, the maximum number of tcp connections allowed is 1024.
* Because the driver uses `IO#write_nonblock`, Windows is not supported.

Please [refer to the usage documentation for more information on common pitfalls](http://datastax.github.io/ruby-driver/features/)

## Contributing

For contributing read [CONTRIBUTING.md](https://github.com/datastax/ruby-driver/blob/master/CONTRIBUTING.md)

## Credits

This driver is based on the original work of [Theo Hultberg](https://github.com/iconara)
on [cql-rb](https://github.com/iconara/cql-rb/) and adds a series of advanced features
that are common across all other DataStax drivers for Apache Cassandra.

The development effort to provide an up to date, high performance, fully featured Ruby
Driver for Apache Cassandra will continue on this project, while
[cql-rb](https://github.com/iconara/cql-rb/) will be discontinued.

## Copyright

Copyright 2013-2016 DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

  [1]: http://datastax.github.io/ruby-driver/api
