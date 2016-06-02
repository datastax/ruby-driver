# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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
    module V3
      class Encoder
        HEADER_FORMAT = 'c2ncN'.freeze

        def initialize(compressor = nil, protocol_version = 3)
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

          header = [@protocol_version, flags, stream_id, request.opcode, body.bytesize]
          buffer << header.pack(HEADER_FORMAT)
          buffer << body

          buffer
        end

        def write_parameters(buffer, params, types, names = EMPTY_LIST)
          Coder.write_values_v3(buffer, params, types, names)
        end
      end

      class Decoder
        def initialize(handler, compressor = nil)
          @handler    = handler
          @compressor = compressor
          @state      = :initial
          @header     = nil
          @version    = nil
          @code       = nil
          @length     = nil
          @buffer     = CqlByteBuffer.new
        end

        def <<(data)
          @buffer << data

          __send__(:"decode_#{@state}", @buffer)
        end

        private

        READY = ReadyResponse.new

        def decode_initial(buffer)
          return if buffer.length < 9

          frame_header     = buffer.read_int
          protocol_version = (frame_header >> 24) & 0x7f

          if protocol_version < 3
            stream_id  = (frame_header >> 8) & 0xff
            stream_id  = (stream_id & 0x7f) - (stream_id & 0x80)

            error_response = ErrorResponse.new(nil, nil, 0x000A,
                                               'Invalid or unsupported protocol version')
            @handler.complete_request(stream_id, error_response)

            return
          end

          @header = frame_header
          @code   = buffer.read_byte
          @length = buffer.read_int
          @state  = :body

          decode_body(buffer)
        end

        def decode_header(buffer)
          buffer_length = buffer.length

          while buffer_length >= 9
            frame_header = buffer.read_int
            frame_code   = buffer.read_byte
            frame_length = buffer.read_int

            if (buffer_length - 9) < frame_length
              @header = frame_header
              @code   = frame_code
              @length = frame_length
              @state  = :body

              return
            end

            actual_decode(buffer, frame_header, frame_length, frame_code)
            buffer_length = buffer.length
          end

          nil
        end

        def decode_body(buffer)
          frame_header  = @header
          frame_code    = @code
          frame_length  = @length
          buffer_length = buffer.length

          until buffer_length < frame_length
            actual_decode(buffer, frame_header, frame_length, frame_code)
            buffer_length = buffer.length

            if buffer_length < 9
              @header = nil
              @code   = nil
              @length = nil
              @state  = :header

              return
            end

            frame_header   = buffer.read_int
            frame_code     = buffer.read_byte
            frame_length   = buffer.read_int
            buffer_length -= 9
          end

          @header = frame_header
          @code   = frame_code
          @length = frame_length

          nil
        end

        def actual_decode(buffer, fields, frame_length, code)
          protocol_version = (fields >> 24) & 0x7f
          compression      = (fields >> 16) & 0x01
          tracing          = (fields >> 16) & 0x02
          stream_id        = fields & 0xffff
          stream_id        = (stream_id & 0x7fff) - (stream_id & 0x8000)
          opcode           = code & 0xff

          # If we're dealing with a compressed body, read the whole body, decompress,
          # and treat the uncompressed body as if that's what we got in the first place.
          # This means we must reset frame_length to that uncompressed size.
          if compression == 1
            if @compressor
              buffer = CqlByteBuffer.new(
                @compressor.decompress(buffer.read(frame_length)))
              frame_length = buffer.size
            else
              raise Errors::DecodingError,
                    'Compressed frame received, but no compressor configured'
            end
          end

          # We want to read one full frame; but after we read/parse chunks of the body
          # there may be more cruft left in the frame that we don't care about. So,
          # we save off the current size of the buffer, do all our reads for the
          # frame, get the final remaining size, and based on that discard possible
          # remaining bytes in the frame. In particular, we account for the possibility
          # that the buffer contains some/all of a subsequent frame as well, and we
          # don't want to mess with that.

          buffer_starting_length = buffer.length

          trace_id = (buffer.read_uuid if tracing == 2)

          remaining_frame_length = frame_length -
                                   (buffer_starting_length - buffer.length)
          response = decode_response(opcode, protocol_version, buffer,
                                     remaining_frame_length, trace_id)

          # Calculate and discard remaining cruft in the frame.
          extra_length = frame_length - (buffer_starting_length - buffer.length)
          buffer.discard(extra_length) if extra_length > 0

          if stream_id == -1
            @handler.notify_event_listeners(response)
          else
            @handler.complete_request(stream_id, response)
          end
        end

        def decode_response(opcode, protocol_version, buffer, size, trace_id)
          case opcode
          when 0x00 # ERROR
            code = buffer.read_int
            message = buffer.read_string

            case code
            when 0x1000
              UnavailableErrorResponse.new(nil,
                                           nil,
                                           code,
                                           message,
                                           buffer.read_consistency,
                                           buffer.read_int,
                                           buffer.read_int)
            when 0x1100
              WriteTimeoutErrorResponse.new(nil,
                                            nil,
                                            code,
                                            message,
                                            buffer.read_consistency,
                                            buffer.read_int,
                                            buffer.read_int,
                                            buffer.read_string)
            when 0x1200
              ReadTimeoutErrorResponse.new(nil,
                                           nil,
                                           code,
                                           message,
                                           buffer.read_consistency,
                                           buffer.read_int,
                                           buffer.read_int,
                                           (buffer.read_byte != 0))
            when 0x2400
              AlreadyExistsErrorResponse.new(nil,
                                             nil,
                                             code,
                                             message,
                                             buffer.read_string,
                                             buffer.read_string)
            when 0x2500
              UnpreparedErrorResponse.new(nil,
                                          nil,
                                          code,
                                          message,
                                          buffer.read_short_bytes)
            else
              ErrorResponse.new(nil, nil, code, message)
            end
          when 0x02 # READY
            READY
          when 0x03 # AUTHENTICATE
            AuthenticateResponse.new(buffer.read_string)
          when 0x06 # SUPPORTED
            SupportedResponse.new(buffer.read_string_multimap)
          when 0x08 # RESULT
            result_type = buffer.read_int
            case result_type
            when 0x0001 # Void
              VoidResultResponse.new(nil, nil, trace_id)
            when 0x0002 # Rows
              original_buffer_length = buffer.length
              column_specs, paging_state = Coder.read_metadata_v3(buffer)

              if column_specs.nil?
                consumed_bytes = original_buffer_length - buffer.length
                remaining_bytes =
                  CqlByteBuffer.new(buffer.read(size - consumed_bytes - 4))
                RawRowsResultResponse.new(nil,
                                          nil,
                                          protocol_version,
                                          remaining_bytes,
                                          paging_state,
                                          trace_id,
                                          nil)
              else
                RowsResultResponse.new(nil,
                                       nil,
                                       Coder.read_values_v3(buffer, column_specs),
                                       column_specs,
                                       paging_state,
                                       trace_id)
              end
            when 0x0003 # SetKeyspace
              SetKeyspaceResultResponse.new(nil, nil, buffer.read_string, trace_id)
            when 0x0004 # Prepared
              id = buffer.read_short_bytes
              params_metadata = Coder.read_metadata_v3(buffer).first
              result_metadata = nil
              result_metadata = Coder.read_metadata_v3(buffer).first if protocol_version > 1

              PreparedResultResponse.new(nil,
                                         nil,
                                         id,
                                         params_metadata,
                                         result_metadata,
                                         nil,
                                         trace_id)
            when 0x0005 # SchemaChange
              change = buffer.read_string
              target = buffer.read_string
              keyspace = buffer.read_string
              arguments = EMPTY_LIST
              name = nil

              name = buffer.read_string if target == Constants::SCHEMA_CHANGE_TARGET_TABLE

              SchemaChangeResultResponse.new(nil,
                                             nil,
                                             change,
                                             keyspace,
                                             name,
                                             target,
                                             arguments,
                                             nil)
            else
              raise Errors::DecodingError,
                    "Unsupported result type: #{result_type.inspect}"
            end
          when 0x0C # EVENT
            event_type = buffer.read_string
            case event_type
            when 'SCHEMA_CHANGE'
              change = buffer.read_string
              target = buffer.read_string
              keyspace = buffer.read_string
              name = nil
              arguments = EMPTY_LIST

              name = buffer.read_string if target == Constants::SCHEMA_CHANGE_TARGET_TABLE

              SchemaChangeEventResponse.new(change, keyspace, name, target, arguments)
            when 'STATUS_CHANGE'
              StatusChangeEventResponse.new(buffer.read_string, *buffer.read_inet)
            when 'TOPOLOGY_CHANGE'
              TopologyChangeEventResponse.new(buffer.read_string, *buffer.read_inet)
            else
              raise Errors::DecodingError,
                    "Unsupported event type: #{event_type.inspect}"
            end
          when 0x0E # AUTH_CHALLENGE
            AuthChallengeResponse.new(buffer.read_bytes)
          when 0x10 # AUTH_SUCCESS
            AuthSuccessResponse.new(buffer.read_bytes)
          else
            raise Errors::DecodingError,
                  "Unsupported response opcode: #{opcode.inspect}"
          end
        end
      end
    end
  end
end
