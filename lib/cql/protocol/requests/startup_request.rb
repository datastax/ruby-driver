# encoding: utf-8

module Cql
  module Protocol
    class StartupRequest < Request
      def initialize(cql_version='3.0.0', compression=nil)
        super(1)
        @arguments = {CQL_VERSION => cql_version}
        @arguments[COMPRESSION] = compression if compression
      end

      def write(io)
        write_string_map(io, @arguments)
        io
      end

      def to_s
        %(STARTUP #@arguments)
      end

      private

      CQL_VERSION = 'CQL_VERSION'.freeze
      COMPRESSION = 'COMPRESSION'.freeze
    end
  end
end
