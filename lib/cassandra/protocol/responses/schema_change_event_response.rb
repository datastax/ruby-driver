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

module Cassandra
  module Protocol
    class SchemaChangeEventResponse < EventResponse
      TYPE = 'SCHEMA_CHANGE'.freeze

      attr_reader :type, :change, :keyspace, :table

      def initialize(*args)
        @change, @keyspace, @table = args
        @type = TYPE
      end

      def self.decode(protocol_version, buffer, length, trace_id=nil)
        new(buffer.read_string, buffer.read_string, buffer.read_string)
      end

      def eql?(rs)
        rs.type == self.type && rs.change == self.change && rs.keyspace == self.keyspace && rs.table == self.table
      end
      alias_method :==, :eql?

      def hash
        @h ||= begin
          h = 0
          h = ((h & 33554431) * 31) ^ @type.hash
          h = ((h & 33554431) * 31) ^ @change.hash
          h = ((h & 33554431) * 31) ^ @keyspace.hash
          h = ((h & 33554431) * 31) ^ @table.hash
          h
        end
      end

      def to_s
        %(EVENT #@type #@change "#@keyspace" "#@table")
      end

      private

      EVENT_TYPES[TYPE] = self
    end
  end
end
