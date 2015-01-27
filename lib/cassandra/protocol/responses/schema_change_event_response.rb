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
    class SchemaChangeEventResponse < EventResponse
      TYPE = 'SCHEMA_CHANGE'.freeze

      attr_reader :change, :keyspace, :table, :type_name, :target

      def initialize(change, keyspace, name, target = nil)
        @change   = change
        @keyspace = keyspace

        if target
          @target = target
          @table = @type_name = name
        else
          if name.empty?
            @target = Constants::SCHEMA_CHANGE_TARGET_KEYSPACE
          else
            @target = Constants::SCHEMA_CHANGE_TARGET_TABLE
            @table  = name
          end
        end
      end

      def type
        TYPE
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
        %(EVENT SCHEMA_CHANGE #@change #@target "#@keyspace" "#@table")
      end

      private

      EVENT_TYPES[TYPE] = self
    end
  end
end
