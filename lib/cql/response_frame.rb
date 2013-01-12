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
        @body = create_body if @headers.complete?
      end
    end

    private

    def create_body
      body_type = begin
        case @headers.opcode
        when 0x00 then ErrorResponse
        when 0x02 then ReadyResponse
        when 0x06 then SupportedResponse
        else
          raise UnsupportedOperationError, "The operation #{@headers.opcode} is not supported"
        end
      end
      FrameBody.new(@headers.buffer, @headers.length, body_type)
    end

    class FrameHeaders
      attr_reader :buffer, :protocol_version, :opcode, :length

      def initialize(buffer)
        @buffer = buffer
        check_complete!
      end

      def <<(str)
        @buffer << str
        check_complete!
      end

      def complete?
        !!@protocol_version
      end

      private

      HEADER_FORMAT = 'C4N'.freeze

      def check_complete!
        if @buffer.length >= 8
          @protocol_version, @flags, @stream_id, @opcode, @length = @buffer.slice!(0, 8).unpack(HEADER_FORMAT)
          raise UnsupportedFrameTypeError, 'Request frames are not supported' if @protocol_version & 0x80 == 0
          @protocol_version &= 0x7f
        end
      end
    end

    class FrameBody
      attr_reader :response

      def initialize(buffer, length, type)
        @buffer = buffer
        @length = length
        @type = type
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
          @response = @type.new(@buffer)
        end
      end
    end
  end

  class ResponseBody
    include Decoding

    def initialize(buffer)
      decode!(buffer)
    end

    private

    def decode!(buffer)
    end
  end

  class ErrorResponse < ResponseBody
    attr_reader :code, :message

    def error?
      true
    end

    def to_s
      %(ERROR #{code} "#{message}")
    end

    private

    def decode!(buffer)
      @code = read_int!(buffer)
      @message = read_string!(buffer)
    end
  end

  class ReadyResponse < ResponseBody
    def ready?
      true
    end

    def to_s
      'READY'
    end
  end

  class SupportedResponse < ResponseBody
    attr_reader :options

    def to_s
      %(SUPPORTED #{options})
    end

    private

    def decode!(buffer)
      @options = read_string_multimap!(buffer)
    end
  end
end