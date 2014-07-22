# encoding: utf-8

module Cql
  module Execution
    class Info
      attr_reader :hosts, :consistency, :attempts, :trace

      def initialize(hosts, consistency, attempts, trace)
        @hosts       = hosts
        @consistency = consistency
        @attempts    = attempts
        @trace       = trace
      end
    end
  end
end
