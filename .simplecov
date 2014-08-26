if ENV.include?('TRAVIS')
  Coveralls.wear!
  SimpleCov.formatter = Coveralls::SimpleCov::Formatter
end

SimpleCov.start do
  load_profile 'root_filter'

  add_group 'Source', 'lib'
  add_group 'Unit tests', 'spec/cassandra'
  add_group 'Integration tests', 'spec/integration'
  add_group 'End-to-end tests', 'features'

  use_merging
  merge_timeout 3600
end
