# encoding: utf-8

module Cql
  module Protocol
    CONSISTENCIES = [:any, :one, :two, :three, :quorum, :all, :local_quorum, :each_quorum].freeze
    HEADER_FORMAT = 'c4N'.freeze
  end
end

require 'cql/protocol/encoding'
require 'cql/protocol/decoding'
require 'cql/protocol/response_frame'
require 'cql/protocol/request_frame'