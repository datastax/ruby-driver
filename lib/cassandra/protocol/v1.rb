# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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
  module Protocol
    # @private
    module V1
      class Encoder
        HEADER_FORMAT = 'c4N'.freeze

        def initialize(compressor = nil, protocol_version = 1)
          @compressor       = compressor
          @protocol_version = protocol_version
        end

        def encode(buffer, request, stream_id)
          flags = request.trace? ? 2 : 0
          body  = request.write(CqlByteBuffer.new, @protocol_version, self)

          if @compressor && request.compressable? && @compressor.compress?(body)
            flags |= 1
            body   = @compressor.compress(body)
          end

          header  = [@protocol_version, flags, stream_id, request.opcode, body.bytesize]
          buffer << header.pack(HEADER_FORMAT)
          buffer << body
          buffer
        end

        def write_parameters(buffer, params, types, names = EMPTY_LIST)
          Coder.write_values_v1(buffer, params, types)
        end
      end

      class Decoder
        def initialize(handler, compressor = nil)
          @handler    = handler
          @compressor = compressor
          @state      = :header
          @header     = nil
          @length     = nil
          @buffer     = CqlByteBuffer.new
        end

        def <<(data)
          @buffer << data

          __send__(:"decode_#{@state}", @buffer)
        end

        private

        READY = ReadyResponse.new

        def decode_header(buffer)
          buffer_length = buffer.length

          while buffer_length >= 8
            frame_header = buffer.read_int
            frame_length = buffer.read_int

            if (buffer_length - 8) < frame_length
              @header = frame_header
              @length = frame_length
              @state  = :body

              return
            end

            actual_decode(buffer, frame_header, frame_length)
            buffer_length = buffer.length
          end

          nil
        end

        def decode_body(buffer)
          frame_header  = @header
          frame_length  = @length
          buffer_length = buffer.length

          until buffer_length < frame_length
            actual_decode(buffer, frame_header, frame_length)
            buffer_length = buffer.length

            if buffer_length < 8
              @header = nil
              @length = nil
              @state  = :header

              return
            end

            frame_header   = buffer.read_int
            frame_length   = buffer.read_int
            buffer_length -= 8
          end

          @header = frame_header
          @length = frame_length

          nil
        end

        def actual_decode(buffer, fields, size)
          protocol_version = (fields >> 24) & 0x7f
          compression      = (fields >> 16) & 0x01
          tracing          = (fields >> 16) & 0x02
          stream_id        = (fields >> 8) & 0xff
          stream_id        = (stream_id & 0x7f) - (stream_id & 0x80)
          opcode           = fields & 0xff

          if compression == 1
            if @compressor
              buffer = CqlByteBuffer.new(@compressor.decompress(buffer.read(size)))
              size   = buffer.size
            else
              raise Errors::DecodingError, 'Compressed frame received, but no compressor configured'
            end
          end

          if tracing == 2
            trace_id = buffer.read_uuid
            size    -= 16
          else
            trace_id = nil
          end

          extra_length = buffer.length - size
          response = decode_response(opcode, protocol_version, buffer, size, trace_id)

          if buffer.length > extra_length
            buffer.discard(buffer.length - extra_length)
          end

          if stream_id == -1
            @handler.notify_event_listeners(response)
          else
            @handler.complete_request(stream_id, response)
          end
        end

        private

        CODE_ERROR          = 0x00
        CODE_READY          = 0x02
        CODE_AUTHENTICATE   = 0x03
        CODE_SUPPORTED      = 0x06
        CODE_RESULT         = 0x08
        CODE_EVENT          = 0x0c
        CODE_AUTH_CHALLENGE = 0x0e
        CODE_AUTH_SUCCESS   = 0x10

        def decode_response(opcode, protocol_version, buffer, size, trace_id)
          response = case opcode
          when CODE_READY          then READY
          when CODE_AUTHENTICATE   then AuthenticateResponse.new(buffer.read_string)
          when CODE_AUTH_CHALLENGE then AuthChallengeResponse.new(buffer.read_bytes)
          when CODE_AUTH_SUCCESS   then AuthSuccessResponse.new(buffer.read_bytes)
          when CODE_SUPPORTED      then SupportedResponse.new(buffer.read_string_multimap)
          when CODE_ERROR
            code    = buffer.read_int
            message = buffer.read_string

            case code
            when 0x1000 then UnavailableErrorResponse.new(code, message, buffer.read_consistency, buffer.read_int, buffer.read_int)
            when 0x1100 then WriteTimeoutErrorResponse.new(code, message, buffer.read_consistency, buffer.read_int, buffer.read_int, buffer.read_string)
            when 0x1200 then ReadTimeoutErrorResponse.new(code, message, buffer.read_consistency, buffer.read_int, buffer.read_int, (buffer.read_byte != 0))
            when 0x2400 then AlreadyExistsErrorResponse.new(code, message, buffer.read_string, buffer.read_string)
            when 0x2500 then UnpreparedErrorResponse.new(code, message, buffer.read_short_bytes)
            else
              ErrorResponse.new(code, message)
            end
          when CODE_RESULT
            result_type = buffer.read_int
            case result_type
            when 0x0001 # Void
              VoidResultResponse.new(trace_id)
            when 0x0002 # Rows
              original_buffer_length = buffer.length
              column_specs, paging_state = Coder.read_metadata_v1(buffer)

              if column_specs.nil?
                consumed_bytes  = original_buffer_length - buffer.length
                remaining_bytes = CqlByteBuffer.new(buffer.read(size - consumed_bytes - 4))
                RawRowsResultResponse.new(protocol_version, remaining_bytes, paging_state, trace_id)
              else
                RowsResultResponse.new(Coder.read_values_v1(buffer, column_specs), column_specs, paging_state, trace_id)
              end
            when 0x0003 # SetKeyspace
              SetKeyspaceResultResponse.new(buffer.read_string, trace_id)
            when 0x0004 # Prepared
              id              = buffer.read_short_bytes
              params_metadata = Coder.read_metadata_v1(buffer).first
              result_metadata = nil
              result_metadata = Coder.read_metadata_v1(buffer).first if protocol_version > 1

              PreparedResultResponse.new(id, params_metadata, result_metadata, trace_id)
            when 0x0005 # SchemaChange
              SchemaChangeResultResponse.new(buffer.read_string, buffer.read_string, buffer.read_string, trace_id)
            else
              raise Errors::DecodingError, "Unsupported result type: #{result_type.inspect}"
            end
          when CODE_EVENT
            event_type = buffer.read_string
            case event_type
            when 'SCHEMA_CHANGE'
              SchemaChangeEventResponse.new(buffer.read_string, buffer.read_string, buffer.read_string)
            when 'STATUS_CHANGE'
              StatusChangeEventResponse.new(buffer.read_string, *buffer.read_inet)
            when 'TOPOLOGY_CHANGE'
              TopologyChangeEventResponse.new(buffer.read_string, *buffer.read_inet)
            else
              raise Errors::DecodingError, "Unsupported event type: #{event_type.inspect}"
            end
          else
            raise Errors::DecodingError, "Unsupported response opcode: #{opcode.inspect}"
          end
        end
      end
    end
  end
end
