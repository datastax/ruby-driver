# encoding: utf-8

ENV['CASSANDRA_HOST'] ||= '127.0.0.1'

require 'bundler/setup'

unless ENV['COVERAGE'] == 'no'
  require 'coveralls'
  Coveralls.wear!

  require 'simplecov'
  SimpleCov.start do
    add_group 'Source', 'lib'
    add_group 'Unit tests', 'spec/cql'
    add_group 'Integration tests', 'spec/integration'
    add_group 'Test support', 'spec/support'
  end
end

require 'cql'

require 'support/bytes_helper'
require 'support/await_helper'
require 'support/fake_server'
require 'support/fake_io_reactor'