# encoding: utf-8

module Cql
  module Protocol
    class ErrorResponse < Response
      attr_reader :code, :message

      def initialize(*args)
        @code, @message = args
      end

      def self.decode!(buffer, trace_id=nil)
        code = read_int!(buffer)
        message = read_string!(buffer)
        case code
        when 0x1000, 0x1100, 0x1200, 0x2400, 0x2500
          DetailedErrorResponse.decode!(code, message, buffer)
        else
          new(code, message)
        end
      end

      def to_s
        hex_code = @code.to_s(16).rjust(4, '0').upcase
        %(ERROR 0x#{hex_code} "#@message")
      end
    end
  end
end
