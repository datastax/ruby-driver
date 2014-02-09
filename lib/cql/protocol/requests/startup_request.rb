# encoding: utf-8

module Cql
  module Protocol
    class StartupRequest < Request
      attr_reader :options

      def initialize(cql_version=nil, compression=nil)
        super(1)
        @options = {CQL_VERSION => cql_version || DEFAULT_CQL_VERSION}
        @options[COMPRESSION] = compression if compression
      end

      def compressable?
        false
      end

      def write(protocol_version, io)
        write_string_map(io, @options)
      end

      def to_s
        %(STARTUP #@options)
      end

      private

      DEFAULT_CQL_VERSION = '3.0.0'.freeze
      CQL_VERSION = 'CQL_VERSION'.freeze
      COMPRESSION = 'COMPRESSION'.freeze
    end
  end
end
