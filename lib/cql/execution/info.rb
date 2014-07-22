# encoding: utf-8

module Cql
  module Execution
    class Info
      attr_reader :keyspace, :statement, :options, :hosts, :consistency, :retries, :trace

      def initialize(keyspace, statement, options, hosts, consistency, retries, trace)
        @keyspace    = keyspace
        @statement   = statement
        @options     = options
        @hosts       = hosts
        @consistency = consistency
        @retries     = retries
        @trace       = trace
      end
    end
  end
end
