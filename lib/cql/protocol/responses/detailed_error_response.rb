# encoding: utf-8

module Cql
  module Protocol
    class DetailedErrorResponse < ErrorResponse
      attr_reader :details

      def initialize(code, message, details)
        super(code, message)
        @details = details
      end

      def self.decode(code, message, protocol_version, buffer, length, trace_id=nil)
        details = {}
        case code
        when 0x1000 # unavailable
          details[:cl] = buffer.read_consistency
          details[:required] = buffer.read_int
          details[:alive] = buffer.read_int
        when 0x1100 # write_timeout
          details[:cl] = buffer.read_consistency
          details[:received] = buffer.read_int
          details[:blockfor] = buffer.read_int
          details[:write_type] = buffer.read_string
        when 0x1200 # read_timeout
          details[:cl] = buffer.read_consistency
          details[:received] = buffer.read_int
          details[:blockfor] = buffer.read_int
          details[:data_present] = buffer.read_byte != 0
        when 0x2400 # already_exists
          details[:ks] = buffer.read_string
          details[:table] = buffer.read_string
        when 0x2500
          details[:id] = buffer.read_short_bytes
        end
        new(code, message, details)
      end

      def to_s
        "#{super} #{@details}"
      end
    end
  end
end
