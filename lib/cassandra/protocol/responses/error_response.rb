# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
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
    class ErrorResponse < Response
      # @private
      RESPONSE_TYPES[0x00] = self

      attr_reader :code, :message, :custom_payload, :warnings

      def initialize(*args)
        @custom_payload, @warnings, @code, @message = args
      end

      def to_s
        hex_code = @code.to_s(16).rjust(4, '0').upcase
        %(ERROR 0x#{hex_code} "#{@message}")
      end

      def to_error(keyspace, statement, options, hosts, consistency, retries)
        case @code
        when 0x0000 then Errors::ServerError.new(@message,
                                                 @custom_payload,
                                                 @warnings,
                                                 keyspace,
                                                 statement,
                                                 options,
                                                 hosts,
                                                 consistency,
                                                 retries)
        when 0x000A then Errors::ProtocolError.new(@message,
                                                   @custom_payload,
                                                   @warnings,
                                                   keyspace,
                                                   statement,
                                                   options,
                                                   hosts,
                                                   consistency,
                                                   retries)
        when 0x0100 then Errors::AuthenticationError.new(@message,
                                                         @custom_payload,
                                                         @warnings,
                                                         keyspace,
                                                         statement,
                                                         options,
                                                         hosts,
                                                         consistency,
                                                         retries)
        when 0x1001 then Errors::OverloadedError.new(@message,
                                                     @custom_payload,
                                                     @warnings,
                                                     keyspace,
                                                     statement,
                                                     options,
                                                     hosts,
                                                     consistency,
                                                     retries)
        when 0x1002 then Errors::IsBootstrappingError.new(@message,
                                                          @custom_payload,
                                                          @warnings,
                                                          keyspace,
                                                          statement,
                                                          options,
                                                          hosts,
                                                          consistency,
                                                          retries)
        when 0x1003 then Errors::TruncateError.new(@message,
                                                   @custom_payload,
                                                   @warnings,
                                                   keyspace,
                                                   statement,
                                                   options,
                                                   hosts,
                                                   consistency,
                                                   retries)
        when 0x2000 then Errors::SyntaxError.new(@message,
                                                 @custom_payload,
                                                 @warnings,
                                                 keyspace,
                                                 statement,
                                                 options,
                                                 hosts,
                                                 consistency,
                                                 retries)
        when 0x2100 then Errors::UnauthorizedError.new(@message,
                                                       @custom_payload,
                                                       @warnings,
                                                       keyspace,
                                                       statement,
                                                       options,
                                                       hosts,
                                                       consistency,
                                                       retries)
        when 0x2200 then Errors::InvalidError.new(@message,
                                                  @custom_payload,
                                                  @warnings,
                                                  keyspace,
                                                  statement,
                                                  options,
                                                  hosts,
                                                  consistency,
                                                  retries)
        when 0x2300 then Errors::ConfigurationError.new(@message,
                                                        @custom_payload,
                                                        @warnings,
                                                        keyspace,
                                                        statement,
                                                        options,
                                                        hosts,
                                                        consistency,
                                                        retries)
        else
          Errors::ServerError.new(@message,
                                  @custom_payload,
                                  @warnings,
                                  keyspace,
                                  statement,
                                  options,
                                  hosts,
                                  consistency,
                                  retries)
        end
      end
    end
  end
end
