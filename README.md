# Datastax Ruby Driver for Apache Cassandra

A Ruby client driver for Apache Cassandra. This driver works exclusively with the Cassandra Query Language v3 (CQL3) and Cassandra's native protocol. Cassandra versions 1.2 and 2.0 are supported as well as Ruby 1.9.3, 2.0, JRuby 1.7 and Rubinius 2.1.

## Quick start

```ruby
require 'cassandra'

cluster = Cassandra.connect # connects to localhost by default

cluster.hosts.each do |host| # automatically discovers all peers
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

## Changelog & versioning

Check out the [releases on GitHub](https://github.com/iconara/cql-rb/releases). Version numbering follows the [semantic versioning](http://semver.org/) scheme.

Private and experimental APIs, defined as whatever is not in the [public API documentation][1], i.e. classes and methods marked as `@private`, will change without warning. If you've been recommended to try an experimental API by the maintainers, please let them know if you depend on that API. Experimental APIs will eventually become public, and knowing how they are used helps in determining their maturity.

Prereleases will be stable, in the sense that they will have finished and properly tested features only, but may introduce APIs that will change before the final release. Please use the prereleases and report bugs, but don't deploy them to production without consulting the maintainers, or doing extensive testing yourself. If you do deploy to production please let the maintainers know as this helps determining the maturity of the release.

## Known bugs & limitations

* JRuby 1.6 is not officially supported, although 1.6.8 should work, if you're stuck in JRuby 1.6.8 try and see if it works for you.
* Windows is not supported (there is experimental support in the [`windows` branch](https://github.com/iconara/cql-rb/tree/windows_support)).
* Large results are buffered in memory until the whole response has been loaded, the protocol makes it possible to start to deliver rows to the client code as soon as the metadata is loaded, but this is not supported yet.

## Copyright

Copyright 2013-2014 DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

  [1]: http://riptano.github.io/ruby-driver/api
