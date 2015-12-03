CREATE KEYSPACE system_auth WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true;

CREATE TABLE system_auth.resource_role_permissons_index (
  resource varchar,
  role varchar,
  PRIMARY KEY (resource, role)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'index of db roles with permissions granted on a resource'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 7776000
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system_auth.role_members (
  role varchar,
  member varchar,
  PRIMARY KEY (role, member)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'role memberships lookup table'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 7776000
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system_auth.role_permissions (
  role varchar,
  resource varchar,
  permissions set<varchar>,
  PRIMARY KEY (role, resource)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'permissions granted to db roles'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 7776000
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system_auth.roles (
  role varchar PRIMARY KEY,
  can_login boolean,
  is_superuser boolean,
  member_of set<varchar>,
  salted_hash varchar
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'role definitions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 7776000
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE system_schema WITH replication = {'class': 'LocalStrategy'} AND durable_writes = true;

CREATE TABLE system_schema.aggregates (
  keyspace_name varchar,
  aggregate_name varchar,
  signature frozen <list<varchar>>,
  argument_types frozen <list<varchar>>,
  final_func varchar,
  initcond blob,
  return_type varchar,
  state_func varchar,
  state_type varchar,
  PRIMARY KEY (keyspace_name, aggregate_name, signature)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'user defined aggregate definitions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system_schema.columns (
  keyspace_name varchar,
  table_name varchar,
  column_name varchar,
  column_name_bytes blob,
  component_index int,
  type varchar,
  validator varchar,
  PRIMARY KEY (keyspace_name, table_name, column_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'column definitions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system_schema.dropped_columns (
  keyspace_name varchar,
  table_name varchar,
  column_name varchar,
  dropped_time timestamp,
  type varchar,
  PRIMARY KEY (keyspace_name, table_name, column_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'dropped column registry'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system_schema.functions (
  keyspace_name varchar,
  function_name varchar,
  signature frozen <list<varchar>>,
  argument_names frozen <list<varchar>>,
  argument_types frozen <list<varchar>>,
  body varchar,
  called_on_null_input boolean,
  language varchar,
  return_type varchar,
  PRIMARY KEY (keyspace_name, function_name, signature)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'user defined function definitions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system_schema.indexes (
  keyspace_name varchar,
  table_name varchar,
  index_name varchar,
  index_type varchar,
  options frozen <map<varchar, varchar>>,
  target_columns frozen <set<varchar>>,
  target_type varchar,
  PRIMARY KEY (keyspace_name, table_name, index_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'secondary index definitions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system_schema.keyspaces (
  keyspace_name varchar PRIMARY KEY,
  durable_writes boolean,
  replication frozen <map<varchar, varchar>>
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'keyspace definitions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system_schema.materialized_views (
  keyspace_name varchar,
  table_name varchar,
  view_name varchar,
  clustering_columns frozen <list<varchar>>,
  included_columns frozen <list<varchar>>,
  target_columns frozen <list<varchar>>,
  PRIMARY KEY (keyspace_name, table_name, view_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'materialized views definitions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system_schema.tables (
  keyspace_name varchar,
  table_name varchar,
  bloom_filter_fp_chance double,
  caching frozen <map<varchar, varchar>>,
  comment varchar,
  compaction frozen <map<varchar, varchar>>,
  compression frozen <map<varchar, varchar>>,
  dclocal_read_repair_chance double,
  default_time_to_live int,
  extensions frozen <map<varchar, blob>>,
  flags frozen <set<varchar>>,
  gc_grace_seconds int,
  id uuid,
  max_index_interval int,
  memtable_flush_period_in_ms int,
  min_index_interval int,
  read_repair_chance double,
  speculative_retry varchar,
  PRIMARY KEY (keyspace_name, table_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'table definitions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system_schema.triggers (
  keyspace_name varchar,
  table_name varchar,
  trigger_name varchar,
  trigger_options frozen <map<varchar, varchar>>,
  PRIMARY KEY (keyspace_name, table_name, trigger_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'trigger definitions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system_schema.types (
  keyspace_name varchar,
  type_name varchar,
  field_names frozen <list<varchar>>,
  field_types frozen <list<varchar>>,
  PRIMARY KEY (keyspace_name, type_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'user defined type definitions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE system_distributed WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = true;

CREATE TABLE system_distributed.parent_repair_history (
  parent_id timeuuid PRIMARY KEY,
  columnfamily_names set<varchar>,
  exception_message varchar,
  exception_stacktrace varchar,
  finished_at timestamp,
  keyspace_name varchar,
  requested_ranges set<varchar>,
  started_at timestamp,
  successful_ranges set<varchar>
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'Repair history'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system_distributed.repair_history (
  keyspace_name varchar,
  columnfamily_name varchar,
  id timeuuid,
  coordinator inet,
  exception_message varchar,
  exception_stacktrace varchar,
  finished_at timestamp,
  parent_id timeuuid,
  participants set<inet>,
  range_begin varchar,
  range_end varchar,
  started_at timestamp,
  status varchar,
  PRIMARY KEY ((keyspace_name, columnfamily_name), id)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'Repair history'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE system WITH replication = {'class': 'LocalStrategy'} AND durable_writes = true;

CREATE TABLE system."IndexInfo" (
  table_name varchar,
  index_name varchar,
  value 'org.apache.cassandra.db.marshal.EmptyType',
  PRIMARY KEY (table_name, index_name)
)
WITH COMPACT STORAGE
 AND bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'built column indexes'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system.available_ranges (
  keyspace_name varchar PRIMARY KEY,
  ranges set<blob>
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'available keyspace/ranges during bootstrap/replace that are ready to be served'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system.batches (
  id timeuuid PRIMARY KEY,
  mutations list<blob>,
  version int
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'batches awaiting replay'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '2'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system.batchlog (
  id uuid PRIMARY KEY,
  data blob,
  version int,
  written_at timestamp
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = '*DEPRECATED* batchlog entries'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '2'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system.built_materialized_views (
  keyspace_name varchar,
  view_name varchar,
  PRIMARY KEY (keyspace_name, view_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'built materialized views'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system.compaction_history (
  id uuid PRIMARY KEY,
  bytes_in bigint,
  bytes_out bigint,
  columnfamily_name varchar,
  compacted_at timestamp,
  keyspace_name varchar,
  rows_merged map<int, bigint>
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'week-long compaction history'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 604800
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system.hints (
  target_id uuid,
  hint_id timeuuid,
  message_version int,
  mutation blob,
  PRIMARY KEY (target_id, hint_id, message_version)
)
WITH COMPACT STORAGE
 AND bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = '*DEPRECATED* hints awaiting delivery'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system.local (
  key varchar PRIMARY KEY,
  bootstrapped varchar,
  broadcast_address inet,
  cluster_name varchar,
  cql_version varchar,
  data_center varchar,
  gossip_generation int,
  host_id uuid,
  listen_address inet,
  native_protocol_version varchar,
  partitioner varchar,
  rack varchar,
  release_version varchar,
  rpc_address inet,
  schema_version uuid,
  thrift_version varchar,
  tokens set<varchar>,
  truncated_at map<uuid, blob>
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'information about the local node'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system.materialized_views_builds_in_progress (
  keyspace_name varchar,
  view_name varchar,
  generation_number int,
  last_token varchar,
  PRIMARY KEY (keyspace_name, view_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'materialized views builds current progress'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system.paxos (
  row_key blob,
  cf_id uuid,
  in_progress_ballot timeuuid,
  most_recent_commit blob,
  most_recent_commit_at timeuuid,
  most_recent_commit_version int,
  proposal blob,
  proposal_ballot timeuuid,
  proposal_version int,
  PRIMARY KEY (row_key, cf_id)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'in-progress paxos proposals'
 AND compaction = {'class': 'LeveledCompactionStrategy'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system.peer_events (
  peer inet PRIMARY KEY,
  hints_dropped map<uuid, int>
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'events related to peers'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system.peers (
  peer inet PRIMARY KEY,
  data_center varchar,
  host_id uuid,
  preferred_ip inet,
  rack varchar,
  release_version varchar,
  rpc_address inet,
  schema_version uuid,
  tokens set<varchar>
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'information about known peers in the cluster'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system.range_xfers (
  token_bytes blob PRIMARY KEY,
  requested_at timestamp
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'ranges requested for transfer'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system.size_estimates (
  keyspace_name varchar,
  table_name varchar,
  range_start varchar,
  range_end varchar,
  mean_partition_size bigint,
  partitions_count bigint,
  PRIMARY KEY (keyspace_name, table_name, range_start, range_end)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'per-table primary range size estimates'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system.sstable_activity (
  keyspace_name varchar,
  columnfamily_name varchar,
  generation int,
  rate_120m double,
  rate_15m double,
  PRIMARY KEY ((keyspace_name, columnfamily_name, generation))
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'historic sstable read rates'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE system_traces WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'} AND durable_writes = true;

CREATE TABLE system_traces.events (
  session_id uuid,
  event_id timeuuid,
  activity varchar,
  source inet,
  source_elapsed int,
  thread varchar,
  PRIMARY KEY (session_id, event_id)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'tracing events'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';

CREATE TABLE system_traces.sessions (
  session_id uuid PRIMARY KEY,
  client inet,
  command varchar,
  coordinator inet,
  duration int,
  parameters map<varchar, varchar>,
  request varchar,
  started_at timestamp
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = 'tracing sessions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';