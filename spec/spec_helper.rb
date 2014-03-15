# encoding: utf-8

ENV['CASSANDRA_HOST'] ||= 'localhost'

require 'bundler/setup'

require 'support/bytes_helper'
require 'support/await_helper'
require 'support/fake_io_reactor'

unless ENV['COVERAGE'] == 'no' || RUBY_ENGINE == 'rbx'
  require 'coveralls'
  require 'simplecov'

  if ENV.include?('TRAVIS')
    Coveralls.wear!
    SimpleCov.formatter = Coveralls::SimpleCov::Formatter
  end

  SimpleCov.start do
    add_group 'Source', 'lib'
    add_group 'Unit tests', 'spec/cql'
    add_group 'Integration tests', 'spec/integration'
  end
end

require 'cql'
require 'cql/compression/snappy_compressor'
