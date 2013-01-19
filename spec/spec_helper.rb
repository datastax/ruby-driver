# encoding: utf-8

require 'bundler/setup'
require 'simplecov'; SimpleCov.start
require 'cql'

ENV['CASSANDRA_HOST'] ||= 'localhost'