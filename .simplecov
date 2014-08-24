SimpleCov.start do
  load_profile 'root_filter'

  merge_timeout 1200

  add_group 'Source', 'lib'
  add_group 'Unit tests', 'spec/cql'
  add_group 'Integration tests', 'spec/integration'
  add_group 'Features', 'features'
end

if ENV.include?('TRAVIS')
  Coveralls.wear!
  SimpleCov.formatter = Coveralls::SimpleCov::Formatter
end
