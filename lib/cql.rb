# encoding: utf-8

module Cql
  CqlError = Class.new(StandardError)

  CONSISTENCIES = [:any, :one, :two, :three, :quorum, :all, :local_quorum, :each_quorum].freeze
end

require 'cql/encoding'
require 'cql/decoding'
require 'cql/response_frame'
require 'cql/request_frame'