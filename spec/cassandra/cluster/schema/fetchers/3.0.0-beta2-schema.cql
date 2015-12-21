CREATE KEYSPACE system_auth WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true;

CREATE TABLE system_auth.resource_role_permissons_index (
  resource text,
  role text,
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
  role text,
  member text,
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
  role text,
  resource text,
  permissions set<text>,
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
  role text PRIMARY KEY,
  can_login boolean,
  is_superuser boolean,
  member_of set<text>,
  salted_hash text
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
  keyspace_name text,
  aggregate_name text,
  signature frozen <list<text>>,
  argument_types frozen <list<text>>,
  final_func text,
  initcond blob,
  return_type text,
  state_func text,
  state_type text,
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
  keyspace_name text,
  table_name text,
  column_name text,
  column_name_bytes blob,
  component_index int,
  type text,
  validator text,
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
  keyspace_name text,
  table_name text,
  column_name text,
  dropped_time timestamp,
  type text,
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
  keyspace_name text,
  function_name text,
  signature frozen <list<text>>,
  argument_names frozen <list<text>>,
  argument_types frozen <list<text>>,
  body text,
  called_on_null_input boolean,
  language text,
  return_type text,
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
  keyspace_name text,
  table_name text,
  index_name text,
  index_type text,
  options frozen <map<text, text>>,
  target_columns frozen <set<text>>,
  target_type text,
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
  keyspace_name text PRIMARY KEY,
  durable_writes boolean,
  replication frozen <map<text, text>>
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
  keyspace_name text,
  table_name text,
  view_name text,
  clustering_columns frozen <list<text>>,
  included_columns frozen <list<text>>,
  target_columns frozen <list<text>>,
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
  keyspace_name text,
  table_name text,
  bloom_filter_fp_chance double,
  caching frozen <map<text, text>>,
  comment text,
  compaction frozen <map<text, text>>,
  compression frozen <map<text, text>>,
  dclocal_read_repair_chance double,
  default_time_to_live int,
  extensions frozen <map<text, blob>>,
  flags frozen <set<text>>,
  gc_grace_seconds int,
  id uuid,
  max_index_interval int,
  memtable_flush_period_in_ms int,
  min_index_interval int,
  read_repair_chance double,
  speculative_retry text,
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
  keyspace_name text,
  table_name text,
  trigger_name text,
  trigger_options frozen <map<text, text>>,
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
  keyspace_name text,
  type_name text,
  field_names frozen <list<text>>,
  field_types frozen <list<text>>,
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
  columnfamily_names set<text>,
  exception_message text,
  exception_stacktrace text,
  finished_at timestamp,
  keyspace_name text,
  requested_ranges set<text>,
  started_at timestamp,
  successful_ranges set<text>
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
  keyspace_name text,
  columnfamily_name text,
  id timeuuid,
  coordinator inet,
  exception_message text,
  exception_stacktrace text,
  finished_at timestamp,
  parent_id timeuuid,
  participants set<inet>,
  range_begin text,
  range_end text,
  started_at timestamp,
  status text,
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
  table_name text,
  index_name text,
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
  keyspace_name text PRIMARY KEY,
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
  keyspace_name text,
  view_name text,
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
  columnfamily_name text,
  compacted_at timestamp,
  keyspace_name text,
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
  key text PRIMARY KEY,
  bootstrapped text,
  broadcast_address inet,
  cluster_name text,
  cql_version text,
  data_center text,
  gossip_generation int,
  host_id uuid,
  listen_address inet,
  native_protocol_version text,
  partitioner text,
  rack text,
  release_version text,
  rpc_address inet,
  schema_version uuid,
  thrift_version text,
  tokens set<text>,
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
  keyspace_name text,
  view_name text,
  generation_number int,
  last_token text,
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
  data_center text,
  host_id uuid,
  preferred_ip inet,
  rack text,
  release_version text,
  rpc_address inet,
  schema_version uuid,
  tokens set<text>
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
  keyspace_name text,
  table_name text,
  range_start text,
  range_end text,
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
  keyspace_name text,
  columnfamily_name text,
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
  activity text,
  source inet,
  source_elapsed int,
  thread text,
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
  command text,
  coordinator inet,
  duration int,
  parameters map<text, text>,
  request text,
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