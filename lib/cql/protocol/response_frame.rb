# encoding: utf-8

module Cql
  module Protocol
    class ResponseFrame
      def initialize(buffer=ByteBuffer.new)
        @headers = FrameHeaders.new(buffer)
        check_complete!
      end

      def stream_id
        @headers && @headers.stream_id
      end

      def header_length
        8
      end

      def body_length
        @headers && @headers.length
      end

      def body
        @body.response
      end

      def complete?
        @body && @body.complete?
      end

      def <<(str)
        if @body
          @body << str
        else
          @headers << str
          check_complete!
        end
      end

      private

      def check_complete!
        if @headers.complete?
          @body = create_body
        end
      end

      def create_body
        body_type = begin
          case @headers.opcode
          when 0x00 then ErrorResponse
          when 0x02 then ReadyResponse
          when 0x03 then AuthenticateResponse
          when 0x06 then SupportedResponse
          when 0x08 then ResultResponse
          when 0x0c then EventResponse
          else
            raise UnsupportedOperationError, "The operation #{@headers.opcode} is not supported"
          end
        end
        FrameBody.new(@headers.buffer, @headers.length, body_type, @headers.trace_id)
      end

      class FrameHeaders
        attr_reader :buffer, :protocol_version, :stream_id, :opcode, :length

        def initialize(buffer)
          @buffer = buffer
          check_complete!
        end

        def <<(str)
          @buffer << str
          check_complete!
        end

        def complete?
          !!@protocol_version && (!@tracing || @trace_id)
        end

        def trace_id
          @trace_id
        end

        private

        def check_complete!
          if @buffer.length >= 8
            @protocol_version = @buffer.read_byte(true)
            @flags = @buffer.read_byte(true)
            @stream_id = @buffer.read_byte(true)
            @opcode = @buffer.read_byte(true)
            @length = @buffer.read_int
            raise UnsupportedFrameTypeError, 'Request frames are not supported' if @protocol_version > 0
            @protocol_version &= 0x7f
            @tracing = (@flags & 2) == 2
            if @tracing && @buffer.length >= 16
              @trace_id = Decoding.read_uuid!(@buffer)
              @length -= 16
            end
          end
        end
      end

      class FrameBody
        attr_reader :response, :buffer

        def initialize(buffer, length, type, trace_id)
          @buffer = buffer
          @length = length
          @type = type
          @trace_id = trace_id
          check_complete!
        end

        def <<(str)
          @buffer << str
          check_complete!
        end

        def complete?
          !!@response
        end

        private

        def check_complete!
          if @buffer.length >= @length
            extra_length = @buffer.length - @length
            @response = @type.decode!(@buffer, @trace_id)
            if @buffer.length > extra_length
              @buffer.discard(@buffer.length - extra_length)
            end
          end
        end
      end
    end
  end
end
