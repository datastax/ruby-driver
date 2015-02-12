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
    class ReadTimeoutErrorResponse < ErrorResponse
      attr_reader :consistency, :received, :blockfor, :data_present

      def initialize(code, message, consistency, received, blockfor, data_present)
        super(code, message)

        @consistency  = consistency
        @received     = received
        @blockfor     = blockfor
        @data_present = data_present
      end

      def to_error(statement = nil)
        Errors::ReadTimeoutError.new(@message, statement, @data_present, @consistency, @blockfor, @received)
      end

      def to_s
        "#{super} #{@consistency} #{@received} #{@blockfor} #{@data_present}"
      end
    end
  end
end
