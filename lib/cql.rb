# encoding: utf-8

module Cql
  CqlError = Class.new(StandardError)
end

require 'cql/encoding'
require 'cql/decoding'
require 'cql/response_frame'
require 'cql/request_frame'