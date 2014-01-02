# encoding: utf-8

module Cql
  module Protocol
    class DetailedErrorResponse < ErrorResponse
      attr_reader :details

      def initialize(code, message, details)
        super(code, message)
        @details = details
      end

      def self.decode!(code, message, protocol_version, buffer, trace_id=nil)
        details = {}
        case code
        when 0x1000 # unavailable
          details[:cl] = read_consistency!(buffer)
          details[:required] = read_int!(buffer)
          details[:alive] = read_int!(buffer)
        when 0x1100 # write_timeout
          details[:cl] = read_consistency!(buffer)
          details[:received] = read_int!(buffer)
          details[:blockfor] = read_int!(buffer)
          details[:write_type] = read_string!(buffer)
        when 0x1200 # read_timeout
          details[:cl] = read_consistency!(buffer)
          details[:received] = read_int!(buffer)
          details[:blockfor] = read_int!(buffer)
          details[:data_present] = read_byte!(buffer) != 0
        when 0x2400 # already_exists
          details[:ks] = read_string!(buffer)
          details[:table] = read_string!(buffer)
        when 0x2500
          details[:id] = read_short_bytes!(buffer)
        end
        new(code, message, details)
      end

      def to_s
        "#{super} #{@details}"
      end
    end
  end
end
