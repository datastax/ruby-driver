# encoding: utf-8

module Cql
  module Protocol
    class FrameDecoder
      def initialize(compressor=nil)
        @compressor = compressor
      end

      def decode_frame(buffer, partial_frame=nil)
        partial_frame ||= NULL_FRAME
        if partial_frame == NULL_FRAME
          return NULL_FRAME if buffer.length < 8
          fields = buffer.read_int
          size = buffer.read_int
          if buffer.length >= size
            actual_decode(buffer, fields, size)
          else
            PartialFrame.new(fields, size)
          end
        elsif buffer.length >= partial_frame.size
          actual_decode(buffer, partial_frame.fields, partial_frame.size)
        else
          partial_frame
        end
      end

      private

      def actual_decode(buffer, fields, size)
        if (fields >> 24) & 0x80 == 0
          raise UnsupportedFrameTypeError, 'Request frames are not supported'
        end
        protocol_version = (fields >> 24) & 0x7f
        compression = (fields >> 16) & 0x01
        tracing = (fields >> 16) & 0x02
        stream_id = (fields >> 8) & 0xff
        stream_id = (stream_id & 0x7f) - (stream_id & 0x80)
        opcode = fields & 0xff
        if compression == 1
          if @compressor
            compressed_body = buffer.read(size)
            decompressed_body = @compressor.decompress(compressed_body)
            buffer = ByteBuffer.new(decompressed_body)
            size = buffer.length
          else
            raise UnexpectedCompressionError, 'Compressed frame received, but no compressor configured'
          end
        end
        extra_length = buffer.length - size 
        trace_id = tracing == 2 ? Decoding.read_uuid!(buffer) : nil
        response = Response.decode!(opcode, buffer, trace_id)
        if buffer.length > extra_length
          buffer.discard(buffer.length - extra_length)
        end
        CompleteFrame.new(stream_id, response)
      end

      class NullFrame
        def size
          nil
        end

        def complete?
          false
        end
      end

      class PartialFrame
        attr_reader :fields, :size

        def initialize(fields, size)
          @fields = fields
          @size = size
        end

        def stream_id
          nil
        end

        def complete?
          false
        end
      end

      class CompleteFrame
        attr_reader :stream_id, :body

        def initialize(stream_id, body)
          @stream_id = stream_id
          @body = body
        end

        def complete?
          true
        end
      end

      NULL_FRAME = NullFrame.new
    end
  end
end
