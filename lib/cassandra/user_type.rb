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
  # A user-defined type
  class UserType
    # @private
    class Field
      def initialize(name, type)
        @name = name
        @type = type
      end

      def to_cql
        "#{@name} #{Util.type_to_cql(@type)}"
      end
    end

    attr_reader :keyspace
    attr_reader :name

    # @private
    def initialize(keyspace, name, fields)
      @keyspace = keyspace
      @name     = name
      @fields   = fields
    end

    # @param name [String] field name
    # @return [Boolean] whether this type has a given field
    def has_field?(name)
      @fields.has_key?(name)
    end

    # Yield or enumerate each field defined in this type
    # @overload each_field
    #   @yieldparam field [Cassandra::Field] current field
    #   @return [Cassandra::UserType] self
    # @overload each_field
    #   @return [Array<String>] a list of fields
    def each_field(&block)
      if block_given?
        @fields.each_key(&block)
        self
      else
        @fields.keys
      end
    end
    alias :fields :each_field

    def to_cql
      cql   = "CREATE TYPE #{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)} (\n"
      first = true

      @fields.each do |_, field|
        if first
          first = false
        else
          cql << ",\n" unless first
        end
        cql << "  #{field.to_cql}"
      end

      cql << "\n);"

      cql
    end
  end
end
