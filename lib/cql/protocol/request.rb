# encoding: utf-8

module Cql
  module Protocol
    class Request
      include Encoding

      attr_reader :opcode, :trace

      def initialize(opcode, trace=false)
        @opcode = opcode
        @trace = trace
      end

      def encode_frame(stream_id=0, buffer=ByteBuffer.new)
        raise InvalidStreamIdError, 'The stream ID must be between 0 and 127' unless 0 <= stream_id && stream_id < 128
        offset = buffer.bytesize
        flags = @trace ? 2 : 0
        buffer << [1, flags, stream_id, opcode, 0].pack(Formats::HEADER_FORMAT)
        write(buffer)
        buffer.update(offset + 4, [(buffer.bytesize - offset - 8)].pack(Formats::INT_FORMAT))
        buffer
      end

      def self.change_stream_id(new_stream_id, buffer, offset=0)
        buffer.update(offset + 2, new_stream_id.chr)
      end
    end
  end
end
