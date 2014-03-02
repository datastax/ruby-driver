# encoding: utf-8

module Cql
  module Protocol
    class StartupRequest < Request
      attr_reader :options

      def initialize(cql_version, compression=nil)
        super(1)
        raise ArgumentError, "Invalid CQL version: #{cql_version.inspect}" unless cql_version
        @options = {CQL_VERSION => cql_version}
        @options[COMPRESSION] = compression if compression
      end

      def compressable?
        false
      end

      def write(protocol_version, buffer)
        buffer.append_string_map(@options)
      end

      def to_s
        %(STARTUP #@options)
      end

      private

      CQL_VERSION = 'CQL_VERSION'.freeze
      COMPRESSION = 'COMPRESSION'.freeze
    end
  end
end
