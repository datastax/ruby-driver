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
