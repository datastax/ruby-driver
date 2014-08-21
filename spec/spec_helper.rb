# encoding: utf-8

ENV['CASSANDRA_HOST'] ||= 'localhost'

require 'bundler/setup'

require 'rspec/collection_matchers'

require 'support/bytes_helper'
require 'support/await_helper'
require 'support/fake_io_reactor'
require 'support/fake_cluster_registry'

unless ENV['COVERAGE'] == 'no' || RUBY_ENGINE == 'rbx'
  require 'coveralls'
  require 'simplecov'
end

require 'cql'
require 'cql/compression/snappy_compressor'
require 'cql/compression/lz4_compressor'

RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
  config.mock_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
end
