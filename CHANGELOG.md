# 2.1.7
Bug Fixes:
* [RUBY-255](https://datastax-oss.atlassian.net/browse/RUBY-255) ControlConnection.peer_ip ignores peers that are missing critical information in system.peers.

# 2.1.6
Bug Fixes:

* [RUBY-202](https://datastax-oss.atlassian.net/browse/RUBY-202) Allow password authenticator to be used for LDAP authentication. This is actually a backport of
     RUBY-169 for the 3.0.0 release.

# 2.1.5

Features:

* Add support for `type_hints` to override type-guessing for non-prepared statements.

Bug Fixes:

* [RUBY-128](https://datastax-oss.atlassian.net/browse/RUBY-128) Fix decoding of large values in maps, sets and lists.

# 2.1.4

Features:

* [RUBY-90](https://datastax-oss.atlassian.net/browse/RUBY-90) Add support for disabling nagle algorithm (tcp nodelay), enabled by default.
* [RUBY-70](https://datastax-oss.atlassian.net/browse/RUBY-70) Add support for client-side timestamps, disabled by default.
* [RUBY-114](https://datastax-oss.atlassian.net/browse/RUBY-114) Add support for serial consistency in batch requests.

Bug Fixes:

* [RUBY-103](https://datastax-oss.atlassian.net/browse/RUBY-103) Don't regenerate schema metadata for the same replication
  strategies and options
* [RUBY-102](https://datastax-oss.atlassian.net/browse/RUBY-102) Allow custom types in schema metadata
* [RUBY-97](https://datastax-oss.atlassian.net/browse/RUBY-97) Allow disabling of the initial population of schema metadata
* [RUBY-95](https://datastax-oss.atlassian.net/browse/RUBY-95) Speed up generation of large token maps
* [RUBY-116](https://datastax-oss.atlassian.net/browse/RUBY-116) fix thread leak on connection error
* [RUBY-119](https://datastax-oss.atlassian.net/browse/RUBY-119) Use `require 'datastax/cassandra'` to avoid namespace conflicts

Breaking Changes:

* Setting `:synchronize_schema` to `true` will no longer perform the initial
  fetching if schema metadata.

# 2.1.3

Release removing backwards incompatible changes included in 2.1.2

# 2.1.2

Release removing accidental debug code from 2.1.1.

# 2.1.1

Bug Fixes:

* [RUBY-98](https://datastax-oss.atlassian.net/browse/RUBY-98) Use of undefined class variable in `Table#create_partition_key`

# 2.1.0

Features:

* Apache Cassandra native protocol v3
* User-defined types and tuples
* Schema metadata includes user-defined types
* Named arguments
* Public types api for type definition and introspection

Breaking Changes:

* Splat style positional arguments support, deprecated in 2.0.0, has been dropped

Bug Fixes:

* [RUBY-93](https://datastax-oss.atlassian.net/browse/RUBY-93) Reconnection can overflow the stack

# 2.0.1

Bug Fixes:

* [RUBY-87](https://datastax-oss.atlassian.net/browse/RUBY-87) Decoder corrupts incomplete response buffer

# 2.0.0

Features:

* Refactored coding layer to support native protocol v3

Breaking Changes:

* Positional arguments are passed in `:arguments` option to `Session#execute`

# 1.2.0

Bug Fixes:

* [RUBY-83] Timestamps loses microseconds when retrieved from database
* [RUBY-85] Driver doesn't always reconnect

# 1.1.1

Bug Fixes:

* [RUBY-75] Raise error when Batch Statement executed against Cassandra < 2.0
* [RUBY-76] `Cassandra::Future.all` doesn't return a future
* [RUBY-77] Re-introduce `Cassandra::Future.promise`

# 1.1.0

Features:

* Added `Cassandra::LoadBalancing::Policy#teardown`
* Allow disabling of automatic schema metadata synchronization
* Allow manually refreshing schema metadata
* Schema change events processing improvement
* Added host list randomization to prevent hotspots between multiple clients
* Future listeners don't block the reactor

Bug Fixes:

* [RUBY-72] Reactor hangs when retrieving trace inside future listener
* [RUBY-73] Invalid index in load balancing plan after cluster resize
* [RUBY-74] Handle partial disconnects

# 1.0.0

Features:

* Added Cassandra.cluster_async
* Removed unused left-over code from cql-rb
* Add verification of types of parameters when binding a prepared statement

Bug Fixes:

* [RUBY-51] Clear current keyspace in all sessions upon deletion

Breaking Changes:

* Removed cql string interpolation of positional arguments for cassandra < 2.0

# 1.0.0.rc.1

Features:

* Token Aware Data Center Aware Round Robin load balancing is used by default
* Automatic detection of broken connections using heartbeats
* Improved exception class hierarchy and documentation
* Configurable node address resolution with EC2 multi-region support

Bug fixes:

* [RUBY-34] handle empty values without crashing
* [RUBY-41] prevent connections to ignored hosts
* [RUBY-44] and [RUBY-43] correctly escape capitalized keyspaces
* [RUBY-48] handle control connection errors with a backoff and retry
* [RUBY-49] fix bug in Future.all

Breaking changes:

* Most of the error classes have changed, `Cassandra::Errors::QueryError` removed
* Connections to hosts in remote datacenters will be disabled by default
* `Cassandra.connect` has been renamed to `Cassandra.cluster` to avoid confusion
* `Cassandra::TimeUuid::Generator` renamed to `Cassandra::Uuid::Generator` and api has been changed
* Default consistency level has been changed from `:quorum` to `:one`
* Default request execution timeout of 10 seconds has been set

# 1.0.0.beta.3

Bug fixes:

* [RUBY-35] handle ghost entries in system.peers table (CASSANDRA-7825)

# 1.0.0.beta.2

Features:

* TokenAware load balancing policy
* Domain names
* SSL encryption

Bug fixes:

* [RUBY-8] correctly update host status when down/up events received immediately after each other

Breaking changes:

* `Cassandra::LoadBalancing::Policy#setup` is required to be implemented.
* `Cassandra::Cluster#each_host`, `Cassandra::Cluster#each_keyspace`, `Cassandra::Keyspace#each_table` and `Cassandra::Table#each_column` return `Array` or `self`.

# 1.0.0.beta.1

Features:

* Fully asynchronous API
* Single cluster, multiple sessions
* New statements API (Simple, Prepared, Bound and Batch)
* Per-request execution information and tracing
* Base set of policies for load balancing, retry and reconnection as well as ability to write your own
* Host and Schema metadata and state listeners
