# encoding: utf-8

module Cql
  CqlError = Class.new(StandardError)
end

require 'cql/uuid'
require 'cql/connection'
require 'cql/protocol'
