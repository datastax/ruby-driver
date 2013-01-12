# encoding: utf-8

module Cql
  UnsupportedOperationError = Class.new(CqlError)
  UnsupportedFrameTypeError = Class.new(CqlError)
  UnsupportedResultKindError = Class.new(CqlError)
  UnsupportedColumnTypeError = Class.new(CqlError)

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
        when 0x08 then ResultResponse
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
          @response = @type.decode!(@buffer)
        end
      end
    end
  end

  class ResponseBody
    extend Decoding

    def self.decode!(buffer)
    end
  end

  class ErrorResponse < ResponseBody
    attr_reader :code, :message

    def initialize(*args)
      @code, @message = args
    end

    def self.decode!(buffer)
      new(read_int!(buffer), read_string!(buffer))
    end

    def to_s
      %(ERROR #{code} "#{message}")
    end
  end

  class ReadyResponse < ResponseBody
    def self.decode!(buffer)
      new
    end

    def to_s
      'READY'
    end
  end

  class SupportedResponse < ResponseBody
    attr_reader :options

    def initialize(options)
      @options = options
    end

    def self.decode!(buffer)
      new(read_string_multimap!(buffer))
    end

    def to_s
      %(SUPPORTED #{options})
    end
  end

  class ResultResponse < ResponseBody
    attr_reader :change, :keyspace, :table, :rows

    def self.decode!(buffer)
      case read_int!(buffer)
      when 0x01
        VoidResultResponse.decode!(buffer)
      when 0x02
        RowsResultResponse.decode!(buffer)
      when 0x03
        SetKeyspaceResultResponse.decode!(buffer)
      when 0x05
        SchemaChangeResultResponse.decode!(buffer)
      else
        raise UnsupportedResultKindError, %(Unsupported result kind "#{@kind}")
      end
    end
  end

  class VoidResultResponse < ResultResponse
    def self.decode!(buffer)
      new
    end

    def to_s
      %(RESULT void)
    end
  end

  class RowsResultResponse < ResultResponse
    attr_reader :rows

    def initialize(rows)
      @rows = rows
    end

    def self.decode!(buffer)
      flags = read_int!(buffer)
      columns_count = read_int!(buffer)
      if flags & 1 == 1
        global_keyspace_name = read_string!(buffer)
        global_table_name = read_string!(buffer)
      end
      column_specs = columns_count.times.map do
        if global_keyspace_name
          keyspace_name = global_keyspace_name
          table_name = global_table_name
        else
          keyspace_name = read_string!(buffer)
          table_name = read_string!(buffer)
        end
        column_name = read_string!(buffer)
        type = read_option!(buffer) do |id, b|
          case id
          when 0x0d then :varchar
          else
            raise UnsupportedColumnTypeError, %(Unsupported column type #{id})
          end
        end
        [keyspace_name, table_name, column_name, type]
      end
      rows_count = read_int!(buffer)
      rows = []
      rows_count.times do |row_index|
        row = {}
        columns_count.times do |column_index|
          _, _, column_name, type = column_specs[column_index]
          row[column_name] = read_bytes!(buffer)
        end
        rows << row
      end
      new(rows)
    end

    def to_s
      %(RESULT rows ...)
    end
  end

  class SetKeyspaceResultResponse < ResultResponse
    attr_reader :keyspace

    def initialize(keyspace)
      @keyspace = keyspace
    end

    def self.decode!(buffer)
      new(read_string!(buffer))
    end

    def to_s
      %(RESULT set_keyspace "#{@keyspace}")
    end
  end

  class SchemaChangeResultResponse < ResultResponse
    attr_reader :change, :keyspace, :table

    def initialize(*args)
      @change, @keyspace, @table = args
    end

    def self.decode!(buffer)
      new(read_string!(buffer), read_string!(buffer), read_string!(buffer))
    end

    def to_s
      %(RESULT schema_change "#{@change}" "#{@keyspace}" "#{@table}")
    end
  end
end