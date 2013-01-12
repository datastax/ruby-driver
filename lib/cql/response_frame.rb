# encoding: utf-8

module Cql
  UnsupportedOperationError = Class.new(CqlError)
  UnsupportedFrameTypeError = Class.new(CqlError)

  class ResponseFrame
    def initialize
      @headers = FrameHeaders.new('')
    end

    def length
      @headers && @headers.length
    end

    def body
      @body && @body.contents
    end

    def complete?
      @body && @body.complete?
    end

    def <<(str)
      if @body
        @body << str
      else
        @headers << str
        @body = create_body if @headers.complete?
      end
    end

    private

    READY_OP = 0x02
    SUPPORTED_OP = 0x06

    def create_body
      body_class = begin
        case @headers.opcode
        when READY_OP then Ready
        when SUPPORTED_OP then Supported
        else
          raise UnsupportedOperationError, "The operation #{@headers.opcode} is not supported"
        end
      end
      body_class.new(@headers.release_buffer!, @headers.length)
    end

    class FrameHeaders
      attr_reader :protocol_version, :opcode, :length

      def initialize(buffer)
        @buffer = buffer
      end

      def <<(str)
        @buffer << str
        if @buffer.length >= 8
          @protocol_version, @flags, @stream_id, @opcode, @length = @buffer.slice!(0, 8).unpack(HEADER_FORMAT)
          raise UnsupportedFrameTypeError, 'Request frames are not supported' if @protocol_version & 0x80 == 0
          @protocol_version &= 0x7f
        end
      end

      def complete?
        !!@protocol_version
      end

      def release_buffer!
        b = @buffer
        @buffer = nil
        b
      end

      private

      HEADER_FORMAT = 'C4N'.freeze
    end

    module Decoding
      def decode_string_multimap!(str)
        map = {}
        map_size = str.slice!(0, 2).unpack('n')
        map_size.first.times do
          key_length = str.slice!(0, 2).unpack('n')
          key = str.slice!(0, key_length.first)
          values = []
          value_list_size = str.slice!(0, 2).unpack('n')
          value_list_size.first.times do
            value_length = str.slice!(0, 2).unpack('n')
            values << str.slice!(0, value_length.first)
          end
          map[key] = values
        end
        map
      end
    end

    class FrameBody
      include Decoding

      def initialize(buffer, length)
        @buffer = buffer
        @length = length
      end

      def <<(str)
        @buffer << str
      end

      def complete?
        @buffer.length >= @length
      end

      def contents
        @contents ||= decode!
      end

      private

      def decode!
        nil
      end
    end

    class Ready < FrameBody
    end

    class Supported < FrameBody

      private

      def decode!
        decode_string_multimap!(@buffer)
      end
    end
  end
end