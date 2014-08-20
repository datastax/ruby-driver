# encoding: utf-8

ENV['CASSANDRA_HOST'] ||= 'localhost'

require 'bundler/setup'

require 'rspec/collection_matchers'

require 'support/bytes_helper'
require 'support/await_helper'
require 'support/fake_io_reactor'
require 'support/fake_cluster_registry'

RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
  config.mock_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
end

unless ENV['COVERAGE'] == 'no' || RUBY_ENGINE == 'rbx'
  require 'coveralls'
  require 'simplecov'

  if ENV.include?('TRAVIS')
    Coveralls.wear!
    SimpleCov.formatter = Coveralls::SimpleCov::Formatter
  end

  SimpleCov.command_name 'RSpec'
  SimpleCov.start do
    add_group 'Source', 'lib'
    add_group 'Unit tests', 'spec/cql'
    add_group 'Integration tests', 'spec/integration'
    add_group 'Features', 'features'
  end
end

require 'cql'
require 'cql/compression/snappy_compressor'
