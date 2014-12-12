SimpleCov.configure do
  load_profile 'root_filter'

  add_group 'Source', 'lib'
  add_group 'Unit tests', 'spec/cassandra'
  add_group 'End-to-end tests', 'features'

  use_merging
  merge_timeout 3600
end
