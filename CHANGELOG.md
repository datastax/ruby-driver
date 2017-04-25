# 3.1.1

Features:

Bug Fixes:
* [RUBY-291](https://datastax-oss.atlassian.net/browse/RUBY-291) Driver fails to connect to cluster when a table column type has a quoted name.

# 3.1.0
Features:
* Do not mark a host as down if there are active connections.
* Update Keyspace metadata to include collection of indexes defined in the keyspace.
* Update Table metadata to include trigger-collection and view-collection metadata. Also include the cdc attribute,
  introduced in C* 3.8. More details [here.](http://cassandra.apache.org/doc/latest/operating/cdc.html)
* Added execution profiles to encapsulate a group of request execution options.
* Added support for v5 beta protocol. This will always be a "work-in-progress" since the protocol is under
  development and the driver is not necessarily updated to the latest revision of it.
* Make prepared statement cache not be scoped by host and optimistically execute prepared statements on hosts where
  we are not sure the statement is already prepared. The motivation is that in the steady state, all nodes have
  prepared statements already, so there is no need to prepare statements before executing them. If the guess is wrong,
  the client will prepare and execute at that point.
* Expose various cluster attributes with getters.

Bug Fixes:
* [RUBY-235](https://datastax-oss.atlassian.net/browse/RUBY-235) execution_info.retries resets retry count when switching hosts.
* [RUBY-255](https://datastax-oss.atlassian.net/browse/RUBY-255) ControlConnection.peer_ip ignores peers that are missing critical information in system.peers.
* [RUBY-264](https://datastax-oss.atlassian.net/browse/RUBY-264) Table erroneously reported as using compact storage.

# 3.0.3

Bug Fixes:
* [RUBY-241](https://datastax-oss.atlassian.net/browse/RUBY-241) Materialied views sometimes have nil ref to base-table.

# 3.0.2

Bug Fixes:
* [RUBY-219](https://datastax-oss.atlassian.net/browse/RUBY-219) Sometimes get stack trace in metadata.rb due to failure in SortedSet initialization.
* [RUBY-220](https://datastax-oss.atlassian.net/browse/RUBY-220) Improve support for custom types.
* [RUBY-231](https://datastax-oss.atlassian.net/browse/RUBY-231) Driver ignores explicitly specified nil timeout (to indicate no time limit on query execution).
* [RUBY-233](https://datastax-oss.atlassian.net/browse/RUBY-233) Client timeout errors are retried for non-idempotent statements. 

# 3.0.0 GA
Features:
* Increased default request timeout (the `timeout` option to `Cassandra.cluster`), from 10 seconds to 12 seconds 
  because C* defaults to a 10 second timeout internally. The extra two seconds is buffer so that the client can
  report the timeout in the server. This is also consistent with the Java driver.
* Expand :client_timestamps cluster configuration option to allow user to specify his own generator for client timestamps.

Bug Fixes:
* [RUBY-207](https://datastax-oss.atlassian.net/browse/RUBY-207) Get NoMethodError when handling a write-timeout error using a downgrading consistency retry policy.
* [RUBY-214](https://datastax-oss.atlassian.net/browse/RUBY-214) Client timestamps in JRuby are not fine-grained enough, causing timestamp collisions and lost rows in C*.

Breaking Changes:
* The Datacenter-aware load balancing policy (Cassandra::LoadBalancing::Policies::DCAwareRoundRobin) defaults to using
  nodes in the local DC only. In prior releases, the policy would fall back to remote nodes after exhausting local nodes.
  Specify a positive value (or nil for unlimited) for `max_remote_hosts_to_use` when initializing the policy to allow remote node use. 

# 3.0.0 rc2
Features:
* Add protocol_version configuration option to allow the user to force the protocol version to use for communication with nodes.
* Expose listen_address and broadcast_address in `Cassandra::Host` if available.
* Add support for materialized views in the schema metadata.
* Add support for Cassandra indexes in the schema metadata.
* Add or expose the id, options, keyspace, partition_key, clustering_columns, and clustering_order attributes to table and view schema objects.
* Add crc_check_chance and extensions attributes to ColumnContainer options.
* Make cluster configuration options list publicly available. (Thanks, Evan Prothro!)

Bug Fixes:
* [RUBY-161](https://datastax-oss.atlassian.net/browse/RUBY-161) Protocol version negotiation in mixed version clusters should not fall back to v1 unless it is truly warranted.    
* [RUBY-180](https://datastax-oss.atlassian.net/browse/RUBY-180) Column ordering is not deterministic in Table metadata.
* [RUBY-185](https://datastax-oss.atlassian.net/browse/RUBY-185) Internal columns in static-compact and dense tables should be ignored.
* [RUBY-186](https://datastax-oss.atlassian.net/browse/RUBY-186) Custom type column metadata should be parsed properly for C* 3.x schemas. 

# 3.0.0 rc1

Features:
* Add connections_per_local_node, connections_per_remote_node, requests_per_connection cluster configuration options to tune parallel query execution and resource usage.
* Add Cassandra::Logger class to make it easy for users to enable debug logging in the client.

Bug Fixes:
* [RUBY-154](https://datastax-oss.atlassian.net/browse/RUBY-154) Improve batch request performance, which had regressed in 3.0.0 beta1.
* [RUBY-155](https://datastax-oss.atlassian.net/browse/RUBY-155) Request timeout timer should not include request queuing time.
* [RUBY-156](https://datastax-oss.atlassian.net/browse/RUBY-156) Do not drop response frames that follow a frame containing a warning.

# 3.0.0 beta1

Features:

* Added optional time out to Cassandra::Future#get
* Allow skipping bound values or using `Cassandra::UNSET` explicitly.
* Add support for smallint, tinyint, date (`Cassandra::Date`) and time (`Cassandra::Time`) data types.
* Add new errors: `Cassandra::Errors::ReadError`, `Cassandra::Errors::WriteError` and `Cassandra::Errors::FunctionCallError`.
* Include schema metadata for User Defined Functions and User Defined Aggregates.
* Include client ip addresses in request traces, only on Cassandra 3.x.
* Add new retry policy decision `Cassandra::Retry::Policy#try_next_host`.
* Support specifying statement idempotence with the new `:idempotent` option when executing.
* Support sending custom payloads when preparing or executing statements using the new `:payload` option.
* Expose custom payloads received with responses on server exceptions and `Cassandra::Execution::Info` instances.
* Expose server warnings on server exceptions and `Cassandra::Execution::Info` instances.

Bug Fixes:

* [RUBY-143](https://datastax-oss.atlassian.net/browse/RUBY-143) Retry querying system table for metadata of new hosts when prior attempts fail, ultimately enabling use of new hosts.
* [RUBY-150](https://datastax-oss.atlassian.net/browse/RUBY-150) Fixed a protocol decoding error that occurred when multiple messages are available in a stream.
* [RUBY-151](https://datastax-oss.atlassian.net/browse/RUBY-151) Decode incomplete UDTs properly.
* [RUBY-120](https://datastax-oss.atlassian.net/browse/RUBY-120) Tuples and UDTs can be used in sets and hash keys.

Breaking Changes:

* Cassandra::Future#join is now an alias to Cassandra::Future#get and will raise an error if the future is resolved with one.
* Default consistency level is now `LOCAL_ONE`.
* Enable tcp no-delay by default.
* Unavailable errors are retried on the next host in the load balancing plan by default.
* Statement execution no longer retried on timeouts, unless `:idempotent => true` has been specified when executing.

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
