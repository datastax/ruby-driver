# encoding: utf-8

module Cql
  CqlError = Class.new(StandardError)
end

require 'cql/uuid'
require 'cql/byte_buffer'
require 'cql/future'
require 'cql/io'
require 'cql/protocol'
require 'cql/client'
