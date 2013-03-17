# encoding: utf-8

module Cql
  ProtocolError = Class.new(CqlError)

  # @private
  module Protocol
    DecodingError = Class.new(ProtocolError)
    EncodingError = Class.new(ProtocolError)
    InvalidStreamIdError = Class.new(ProtocolError)
    InvalidValueError = Class.new(ProtocolError)
    UnsupportedOperationError = Class.new(ProtocolError)
    UnsupportedFrameTypeError = Class.new(ProtocolError)
    UnsupportedResultKindError = Class.new(ProtocolError)
    UnsupportedColumnTypeError = Class.new(ProtocolError)
    UnsupportedEventTypeError = Class.new(ProtocolError)

    CONSISTENCIES = [:any, :one, :two, :three, :quorum, :all, :local_quorum, :each_quorum].freeze

    module Formats
      CHAR_FORMAT = 'c'.freeze
      DOUBLE_FORMAT = 'G'.freeze
      FLOAT_FORMAT = 'g'.freeze
      INT_FORMAT = 'N'.freeze
      SHORT_FORMAT = 'n'.freeze

      BYTES_FORMAT = 'C*'.freeze
      TWO_INTS_FORMAT = 'NN'.freeze
      HEADER_FORMAT = 'c4N'.freeze
    end

    module Constants
      TRUE_BYTE = "\x01".freeze
      FALSE_BYTE = "\x00".freeze
    end
  end
end

require 'cql/protocol/encoding'
require 'cql/protocol/decoding'
require 'cql/protocol/response_frame'
require 'cql/protocol/request_frame'