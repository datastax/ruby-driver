# encoding: utf-8

module Cql
  module Protocol
    class RequestFrame
      def initialize(body, stream_id=0)
        @body = body
        @stream_id = stream_id
        raise InvalidStreamIdError, 'The stream ID must be between 0 and 127' unless 0 <= @stream_id && @stream_id < 128
      end

      def write(io)
        buffer = @body.write(ByteBuffer.new)
        io << [1, 0, @stream_id, @body.opcode].pack(Formats::HEADER_FORMAT)
        io << [buffer.length].pack(Formats::INT_FORMAT)
        io << buffer
      end
    end
  end
end
