# Datastax Ruby Driver for Apache Cassandra

*If you're reading this on GitHub, please note that this is the readme for the development version and that some
features described here might not yet have been released. You can view the documentation for the latest released
version [here](http://docs.datastax.com/en/developer/ruby-driver/latest).*

[![Build Status](https://travis-ci.org/datastax/ruby-driver.svg?branch=master)](https://travis-ci.org/datastax/ruby-driver)

A Ruby client driver for Apache Cassandra. This driver works exclusively with
the Cassandra Query Language version 3 (CQL3) and Cassandra's native protocol.

- Code: https://github.com/datastax/ruby-driver
- Docs: http://docs.datastax.com/en/developer/ruby-driver
- Jira: https://datastax-oss.atlassian.net/browse/RUBY
- Mailing List: https://groups.google.com/a/lists.datastax.com/forum/#!forum/ruby-driver-user
- IRC: #datastax-drivers on [irc.freenode.net](http://freenode.net>)
- Twitter: Follow the latest news about DataStax Drivers - [@stamhankar999](http://twitter.com/stamhankar999), [@avalanche123](http://twitter.com/avalanche123), [@al3xandru](https://twitter.com/al3xandru)

This driver is based on [the cql-rb gem](https://github.com/iconara/cql-rb) by [Theo Hultberg](https://github.com/iconara) and we added support for:

* [Asynchronous execution](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/asynchronous_io/)
* One-off, [prepared](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/basics/prepared_statements/) and [batch statements](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/basics/batch_statements/)
* Automatic peer discovery and cluster metadata with [support for change notifications](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/state_listeners/)
* Various [load-balancing](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/load_balancing/), [retry](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/retry_policies/) and [reconnection](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/reconnection/) policies with [ability to write your own](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/load_balancing/implementing_a_policy/)
* [SSL encryption](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/security/ssl_encryption/)
* [Flexible and robust error handling](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/error_handling/)
* [Per-request execution information and tracing](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/debugging/)
* [Configurable address resolution](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/address_resolution/)

[Check out the slides from Ruby Driver Explained](https://speakerdeck.com/avalanche123/ruby-driver-explained) for a detailed overview of the Ruby Driver architecture.

## Compatibility

This driver works exclusively with the Cassandra Query Language v3 (CQL3) and Cassandra's native protocol. The current version works with:

* Apache Cassandra versions 1.2, 2.0 and 2.1
* DataStax Enterprise 3.1-4.8
* Ruby (MRI) 2.2, 2.3
* JRuby 1.7

__Note__: JRuby 1.6 is not officially supported, although 1.6.8 should work. Rubinius is not supported.
MRI 1.9.3, 2.0, 2.1, and JRuby 9k are not officially supported, but they should work.

## Quick start

```ruby
require 'cassandra'

cluster = Cassandra.cluster # connects to localhost by default

cluster.each_host do |host| # automatically discovers all peers
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

__Note__: The host you specify is just a seed node, the driver will automatically discover all peers in the cluster.

Read more:

* [`Cassandra.cluster` options](http://docs.datastax.com/en/developer/ruby-driver/2.1/api/#cluster-class_method)
* [`Session#execute_async` options](http://docs.datastax.com/en/developer/ruby-driver/2.1/api/session/#execute_async-instance_method)
* [Usage documentation](http://docs.datastax.com/en/developer/ruby-driver/2.1/features)

## Installation

Install via rubygems

```bash
gem install cassandra-driver
```

Install via Gemfile

```ruby
gem 'cassandra-driver'
```

__Note__: if you want to use compression you should also install [snappy](http://rubygems.org/gems/snappy) or [lz4-ruby](http://rubygems.org/gems/lz4-ruby). [Read more about compression.](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/#compression)


## Upgrading from cql-rb

Some of the new features added to the driver have unfortunately led to changes in the original cql-rb API.
In the examples directory, you can find [an example of how to wrap the ruby driver to achieve almost complete
interface parity with cql-rb](https://github.com/datastax/ruby-driver/blob/v2.1.7/examples/cql-rb-wrapper.rb)
to assist you with gradual upgrade.

## What's new in v2.1
See the [changelog](https://github.com/datastax/ruby-driver/blob/master/CHANGELOG.md) for details on patch releases.

Features:

* Apache Cassandra native protocol v3
* [User-defined types](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/basics/user_defined_types/) and [tuples](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/basics/datatypes/#using-tuples)
* [Schema metadata includes user-defined types](http://docs.datastax.com/en/developer/ruby-driver/2.1/api/keyspace/#type-instance_method)
* [Named arguments](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/basics/prepared_statements/#an-insert-statement-is-prepared-with-named-parameters)
* [Public types api for type definition and introspection](http://docs.datastax.com/en/developer/ruby-driver/2.1/api/types/)
* Add support for disabling nagle algorithm (tcp nodelay), enabled by default.
* Add support for client-side timestamps, disabled by default.
* Add support for serial consistency in batch requests.
* Add support for `type_hints` to override type-guessing for non-prepared statements.


Breaking Changes:

* Splat style positional arguments support, deprecated in 2.0.0, has been dropped
* Setting `:synchronize_schema` to `true` will no longer perform the initial
  fetching if schema metadata.

Bug Fixes:

* [[RUBY-93](https://datastax-oss.atlassian.net/browse/RUBY-93)] Reconnection can overflow the stack
* [[RUBY-95](https://datastax-oss.atlassian.net/browse/RUBY-95)] Speed up generation of large token maps
* [[RUBY-97](https://datastax-oss.atlassian.net/browse/RUBY-97)] Allow disabling of the initial population of schema metadata
* [[RUBY-98](https://datastax-oss.atlassian.net/browse/RUBY-98)] Use of undefined class variable in `Table#create_partition_key`
* [[RUBY-102](https://datastax-oss.atlassian.net/browse/RUBY-102)] Allow custom types in schema metadata
* [[RUBY-103](https://datastax-oss.atlassian.net/browse/RUBY-103)] Don't regenerate schema metadata for the same replication strategies and options
* [[RUBY-116](https://datastax-oss.atlassian.net/browse/RUBY-116)] fix thread leak on connection error
* [[RUBY-119](https://datastax-oss.atlassian.net/browse/RUBY-119)] Use `require 'datastax/cassandra'` to avoid namespace conflicts
* [[RUBY-128](https://datastax-oss.atlassian.net/browse/RUBY-128)] Fix decoding of large values in maps, sets and lists.
* [[RUBY-202](https://datastax-oss.atlassian.net/browse/RUBY-202)] Allow password authenticator to be used for LDAP authentication.
* [[RUBY-255](https://datastax-oss.atlassian.net/browse/RUBY-255)] ControlConnection.peer_ip ignores peers that are missing critical information in system.peers.

## Code examples

The DataStax Ruby Driver uses the awesome [Cucumber Framework](http://cukes.info/) for both end-to-end, or acceptance, testing and constructing documentation. All of the features supported by the driver have appropriate acceptance tests with easy-to-copy code examples in the `features/` directory.

## Running tests

If you don't feel like reading through the following instructions on how to run ruby-driver tests, feel free to [check out .travis.yml for the entire build code](https://github.com/datastax/ruby-driver/blob/master/.travis.yml).

* Check out the driver codebase and install test dependencies:

```bash
git clone https://github.com/datastax/ruby-driver.git
cd ruby-driver
bundle install --without docs
```

* [Install ccm](http://www.datastax.com/dev/blog/ccm-a-development-tool-for-creating-local-cassandra-clusters)

* Run tests:

```bash
bundle exec cucumber # runs end-to-end tests (or bundle exec rake cucumber)
bundle exec rspec # runs unit tests (or bundle exec rake rspec)
bundle exec rake integration # run integration tests
bundle exec rake test # run both as well as integration tests
```

## Changelog & versioning

Check out the [releases on GitHub](https://github.com/datastax/ruby-driver/releases) and [changelog](https://github.com/datastax/ruby-driver/blob/master/CHANGELOG.md). Version numbering follows the [semantic versioning](http://semver.org/) scheme.

Private and experimental APIs, defined as whatever is not in the [public API documentation][1], i.e. classes and methods marked as `@private`, will change without warning. If you've been recommended to try an experimental API by the maintainers, please let them know if you depend on that API. Experimental APIs will eventually become public, and knowing how they are used helps in determining their maturity.

Prereleases will be stable, in the sense that they will have finished and properly tested features only, but may introduce APIs that will change before the final release. Please use the prereleases and report bugs, but don't deploy them to production without consulting the maintainers, or doing extensive testing yourself. If you do deploy to production please let the maintainers know as this helps determining the maturity of the release.

## Known bugs & limitations

* Because the driver reactor is using `IO.select`, the maximum number of tcp connections allowed is 1024.
* Because the driver uses `IO#write_nonblock`, Windows is not supported.

Please [refer to the usage documentation for more information on common pitfalls](http://docs.datastax.com/en/developer/ruby-driver/2.1/features/)

## Contributing

For contributing read [CONTRIBUTING.md](https://github.com/datastax/ruby-driver/blob/master/CONTRIBUTING.md)

## Credits

This driver is based on the original work of [Theo Hultberg](https://github.com/iconara) on [cql-rb](https://github.com/iconara/cql-rb/) and adds a series of advanced features that are common across all other DataStax drivers for Apache Cassandra.

The development effort to provide an up to date, high performance, fully featured Ruby Driver for Apache Cassandra will continue on this project, while [cql-rb](https://github.com/iconara/cql-rb/) has been discontinued.


## Copyright

Copyright 2013-2016 DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

  [1]: http://docs.datastax.com/en/developer/ruby-driver/2.1/api
