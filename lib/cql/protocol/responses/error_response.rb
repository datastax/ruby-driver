# encoding: utf-8

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

module Cql
  module Protocol
    class ErrorResponse < Response
      attr_reader :code, :message

      def initialize(*args)
        @code, @message = args
      end

      def self.decode(protocol_version, buffer, length, trace_id=nil)
        code = buffer.read_int
        message = buffer.read_string
        case code
        when 0x1000, 0x1100, 0x1200, 0x2400, 0x2500
          new_length = length - 4 - 4 - message.bytesize
          DetailedErrorResponse.decode(code, message, protocol_version, buffer, new_length)
        else
          new(code, message)
        end
      end

      def to_s
        hex_code = @code.to_s(16).rjust(4, '0').upcase
        %(ERROR 0x#{hex_code} "#@message")
      end

      private

      RESPONSE_TYPES[0x00] = self
    end
  end
end
