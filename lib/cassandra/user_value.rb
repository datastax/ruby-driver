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
  # A user-defined type representation
  class UserValue
    # @private
    def initialize(keyspace, name, values)
      @keyspace = keyspace
      @name     = name
      @values   = values
    end

    # Allows access to properties of a User-Defined Type.
    #
    # @example Getting and setting values
    #   session.execute("CREATE TYPE address (street text, zipcode int)")
    #   session.execute("CREATE TABLE users (id int PRIMARY KEY, location frozen<address>)")
    #   row     = session.execute("SELECT * FROM users WHERE id = 123").first
    #   address = row['address']
    #
    #   puts address.street
    #   address.street = '123 SomePlace Cir'
    #
    # @overload method_missing(name)
    #   @param name [Symbol] name of the field to lookup.
    #   @return [Object] value of the field.
    # @overload method_missing(name, value)
    #   @param name  [Symbol] name of the field (suffixed with `=`) to set
    #     the value for.
    #   @param value [Symbol] new value for the field.
    #   @return [Cassandra::UserValue] self.
    def method_missing(method, *args, &block)
      return super if block_given? || args.size > 1

      key = method.to_s
      set = key.end_with?('=')

      return super if set && args.empty?

      key.chomp!('=') if set

      return super unless @values.has_key?(key)
      return @values[key] unless set

      @values[key] = args.first

      self
    end

    def respond_to?(method)
      key = method.to_s
      key.chomp!('=')

      return true if @values.has_key?(key)
      super
    end

    # Returns value of the field.
    #
    # @param field [String] name of the field to lookup
    # @return [Object] value of the field.
    def [](field)
      @values[field]
    end

    # Sets value of the field.
    #
    # @param field [String] name of the field to lookup.
    # @param value [Object] new value for the field.
    #
    # @return [Cassandra::UserValue] self.
    def []=(field, value)
      raise ::ArgumentError, "Unsupported field #{field.inspect}" unless @values.has_key?(field)

      @values[field] = value

      self
    end

    def inspect
      "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @keyspace=#{@keyspace} @name=#{@name} @values=#{@values.inspect}>"
    end
  end
end