# encoding: utf-8

module Cql
  CqlError = Class.new(StandardError)

  CONSISTENCIES = [:any, :one, :two, :three, :quorum, :all, :local_quorum, :each_quorum].freeze
  HEADER_FORMAT = 'c4N'.freeze
end

require 'cql/connection'
require 'cql/uuid'
require 'cql/encoding'
require 'cql/decoding'
require 'cql/response_frame'
require 'cql/request_frame'