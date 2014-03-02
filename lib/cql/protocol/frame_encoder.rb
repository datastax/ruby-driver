# encoding: utf-8

module Cql
  module Protocol
    # @private
    class FrameEncoder
      def initialize(protocol_version=1, compressor=nil)
        @protocol_version = protocol_version
        @compressor = compressor
      end

      def encode_frame(request, stream_id=0, buffer=nil)
        raise InvalidStreamIdError, 'The stream ID must be between 0 and 127' unless 0 <= stream_id && stream_id < 128
        buffer ||= CqlByteBuffer.new
        flags = request.trace? ? 2 : 0
        body = request.write(@protocol_version, CqlByteBuffer.new)
        if @compressor && request.compressable? && @compressor.compress?(body)
          flags |= 1
          body = @compressor.compress(body)
        end
        header = [@protocol_version, flags, stream_id, request.opcode, body.bytesize]
        buffer << header.pack(Formats::HEADER_FORMAT)
        buffer << body
        buffer
      end

      def change_stream_id(new_stream_id, buffer, offset=0)
        buffer.update(offset + 2, new_stream_id.chr)
      end
    end
  end
end
