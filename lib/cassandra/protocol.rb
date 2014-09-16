# encoding: utf-8

#--
# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

module Cassandra
  # @private
  ProtocolError = Class.new(Error)

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
    UnsupportedFeatureError = Class.new(ProtocolError)
    UnexpectedCompressionError = Class.new(ProtocolError)
    UnmaterializedRowsError = Class.new(ProtocolError)

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
      PROTOCOL_VERSION = "\x01".freeze
      COMPRESSION_OFF = "\x00".freeze
    end
  end
end

require 'cassandra/protocol/cql_byte_buffer'
require 'cassandra/protocol/custom_type_parser'
require 'cassandra/protocol/type_converter'
require 'cassandra/protocol/response'
require 'cassandra/protocol/responses/auth_challenge_response'
require 'cassandra/protocol/responses/auth_success_response'
require 'cassandra/protocol/responses/error_response'
require 'cassandra/protocol/responses/detailed_error_response'
require 'cassandra/protocol/responses/ready_response'
require 'cassandra/protocol/responses/authenticate_response'
require 'cassandra/protocol/responses/supported_response'
require 'cassandra/protocol/responses/result_response'
require 'cassandra/protocol/responses/void_result_response'
require 'cassandra/protocol/responses/rows_result_response'
require 'cassandra/protocol/responses/raw_rows_result_response'
require 'cassandra/protocol/responses/set_keyspace_result_response'
require 'cassandra/protocol/responses/prepared_result_response'
require 'cassandra/protocol/responses/schema_change_result_response'
require 'cassandra/protocol/responses/event_response'
require 'cassandra/protocol/responses/schema_change_event_response'
require 'cassandra/protocol/responses/status_change_event_response'
require 'cassandra/protocol/responses/topology_change_event_response'
require 'cassandra/protocol/request'
require 'cassandra/protocol/requests/auth_response_request'
require 'cassandra/protocol/requests/batch_request'
require 'cassandra/protocol/requests/startup_request'
require 'cassandra/protocol/requests/credentials_request'
require 'cassandra/protocol/requests/options_request'
require 'cassandra/protocol/requests/register_request'
require 'cassandra/protocol/requests/query_request'
require 'cassandra/protocol/requests/void_query_request'
require 'cassandra/protocol/requests/prepare_request'
require 'cassandra/protocol/requests/execute_request'
require 'cassandra/protocol/frame_encoder'
require 'cassandra/protocol/frame_decoder'
require 'cassandra/protocol/cql_protocol_handler'
