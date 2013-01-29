# encoding: utf-8

module Cql
  CqlError = Class.new(StandardError)
end

require 'cql/uuid'
require 'cql/future'
require 'cql/io'
require 'cql/protocol'
