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
    class SchemaChangeEventResponse < EventResponse
      TYPE = 'SCHEMA_CHANGE'.freeze

      attr_reader :change, :keyspace, :name, :target, :arguments

      def initialize(change, keyspace, name, target, arguments)
        @change    = change
        @keyspace  = keyspace
        @name      = name
        @target    = target
        @arguments = arguments
      end

      def type
        TYPE
      end

      def eql?(other)
        other.is_a?(SchemaChangeEventResponse) && other.type == TYPE &&
          @change == other.change &&
          @keyspace == other.keyspace &&
          @name == other.name &&
          @target == other.target &&
          @arguments == other.arguments
      end
      alias_method :==, :eql?

      def hash
        @h ||= begin
          h = 17
          h = 31 * h + @change.hash
          h = 31 * h + @keyspace.hash
          h = 31 * h + @name.hash
          h = 31 * h + @target.hash
          h = 31 * h + @arguments.hash
          h
        end
      end

      def to_s
        if @arguments
          %(EVENT SCHEMA_CHANGE #@change #@target "#@keyspace" "#@name" #@arguments)
        else
          %(EVENT SCHEMA_CHANGE #@change #@target "#@keyspace" "#@name")
        end
      end

      private

      EVENT_TYPES[TYPE] = self
    end
  end
end
