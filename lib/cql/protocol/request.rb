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

      def compressable?
        true
      end

      def encode_frame(stream_id=0, buffer=nil, compressor=nil)
        raise InvalidStreamIdError, 'The stream ID must be between 0 and 127' unless 0 <= stream_id && stream_id < 128
        buffer ||= ByteBuffer.new
        offset = buffer.bytesize
        flags = @trace ? 2 : 0
        body = write(ByteBuffer.new)
        if compressor && compressable? && compressor.compress?(body)
          flags |= 1
          body = compressor.compress(body)
        end
        header = [1, flags, stream_id, opcode, body.bytesize]
        buffer << header.pack(Formats::HEADER_FORMAT)
        buffer << body
        buffer
      end

      def self.change_stream_id(new_stream_id, buffer, offset=0)
        buffer.update(offset + 2, new_stream_id.chr)
      end
    end
  end
end
