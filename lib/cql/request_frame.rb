# encoding: utf-8

module Cql
  class RequestFrame
    def initialize(body)
      @body = body
    end

    def write(io)
      buffer = [1, 0, 0, @body.opcode, 0].pack(HEADER_FORMAT)
      buffer = @body.write(buffer)
      buffer[4, 4] = [buffer.length - 8].pack(INT_FORMAT)
      io << buffer
    end

    private

    INT_FORMAT = 'N'.freeze
    HEADER_FORMAT = 'C4N'.freeze
  end

  class RequestBody
    include Encoding

    attr_reader :opcode

    def initialize(opcode)
      @opcode = opcode
    end
  end

  class StartupRequest < RequestBody
    def initialize(cql_version='3.0.0', compression=nil)
      super(1)
      @cql_version = cql_version
      @compression = compression
    end

    def write(io)
      arguments = {}
      arguments[CQL_VERSION] = @cql_version
      arguments[COMPRESSION] = @compression if @compression
      write_string_map(io, arguments)
      io
    end

    private

    CQL_VERSION = 'CQL_VERSION'.freeze
    COMPRESSION = 'COMPRESSION'.freeze
  end

  class OptionsRequest < RequestBody
    def initialize
      super(5)
    end

    def write(io)
      io
    end
  end
end