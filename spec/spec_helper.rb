# encoding: utf-8

require 'bundler/setup'
require 'simplecov'; SimpleCov.start
require 'cql'

ENV['CASSANDRA_HOST'] ||= 'localhost'


require 'support/bytes_helper'
require 'support/await_helper'
require 'support/fake_server'
require 'support/fake_io_reactor'