# encoding: utf-8

module Cql
  module Protocol
    class Request
      include Encoding

      attr_reader :opcode

      def initialize(opcode)
        @opcode = opcode
      end

      def encode_frame(stream_id=0, buffer=ByteBuffer.new)
        raise InvalidStreamIdError, 'The stream ID must be between 0 and 127' unless 0 <= stream_id && stream_id < 128
        buffer << [1, 0, stream_id, opcode, 0].pack(Formats::HEADER_FORMAT)
        write(buffer)
        buffer.update(4, [(buffer.bytesize - 8)].pack(Formats::INT_FORMAT))
        buffer
      end
    end
  end
end
