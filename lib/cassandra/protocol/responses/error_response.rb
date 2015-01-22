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
  module Protocol
    class ErrorResponse < Response
      attr_reader :code, :message

      def initialize(*args)
        @code, @message = args
      end

      def to_s
        hex_code = @code.to_s(16).rjust(4, '0').upcase
        %(ERROR 0x#{hex_code} "#@message")
      end

      def to_error(statement = nil)
        case @code
        when 0x0000 then Errors::ServerError.new(@message)
        when 0x000A then Errors::ProtocolError.new(@message)
        when 0x0100 then Errors::AuthenticationError.new(@message)
        when 0x1001 then Errors::OverloadedError.new(@message, statement)
        when 0x1002 then Errors::IsBootstrappingError.new(@message, statement)
        when 0x1003 then Errors::TruncateError.new(@message, statement)
        when 0x2000 then Errors::SyntaxError.new(@message, statement)
        when 0x2100 then Errors::UnauthorizedError.new(@message, statement)
        when 0x2200 then Errors::InvalidError.new(@message, statement)
        when 0x2300 then Errors::ConfigurationError.new(@message, statement)
        else
          Errors::ServerError.new(@message)
        end
      end

      private

      RESPONSE_TYPES[0x00] = self
    end
  end
end
