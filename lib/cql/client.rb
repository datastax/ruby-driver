# encoding: utf-8

module Cql
  class QueryError < CqlError
    attr_reader :code, :cql

    def initialize(code, message, cql=nil)
      super(message)
      @code = code
      @cql = cql
    end
  end

  ClientError = Class.new(CqlError)
  AuthenticationError = Class.new(ClientError)

  module Client
    NotConnectedError = Class.new(ClientError)
    InvalidKeyspaceNameError = Class.new(ClientError)

    def self.connect(options={})
      SynchronousClient.new(AsynchronousClient.new(options)).connect
    end
  end
end

require 'cql/client/column_metadata'
require 'cql/client/result_metadata'
require 'cql/client/query_result'
require 'cql/client/prepared_statement'
require 'cql/client/synchronous_client'
require 'cql/client/asynchronous_client'