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
  # Base class for all cassandra types.
  # @abstract This class exists for documentation purposes only
  class Type
    # @return [Symbol] shorthand type name
    def kind
    end

    # Coerces a given value to this type
    #
    # @param value [Object] value to be coerced
    # @return [Object] a value of this type
    def new(*values)
    end

    # Asserts that a given value is of this type
    # @param value [Object] value to be validated
    # @param message [String] (nil) error message to use when assertion fails
    # @yieldreturn [String] error message to use when assertion fails
    # @raise [ArgumentError] if the value is invalid
    # @return [void]
    def assert(value, message = nil, &block)
    end

    # @return [String] a cassandra representation of this type
    def to_s
    end
  end

  module Types; extend self
    # @private
    class Simple < Type
      attr_reader :kind

      def initialize(kind)
        @kind = kind
      end

      def new(value)
        __send__(:"new_#{@kind}", value)
      end

      def assert(value, message = nil, &block)
        __send__(:"assert_#{@kind}", value, message, &block)
        nil
      end

      def to_s
        @kind.to_s
      end

      alias :to_cql :to_s

      def eql?(other)
        other.is_a?(Simple) && @kind == other.kkind
      end
      alias :== :eql?

      private

      def new_varchar(value)
       String(value)
      end

      def assert_varchar(value, message, &block)
        Util.assert_instance_of(::String, value, message, &block)
      end

      def new_text(value)
        String(value)
      end

      def assert_text(valuee, message, &block)
        Util.assert_instance_of(::String, valuee, message, &block)
      end

      def new_blob(value)
        String(value)
      end

      def assert_blob(valuee, message, &block)
        Util.assert_instance_of(::String, valuee, message, &block)
      end

      def new_ascii(value)
        String(value)
      end

      def assert_ascii(valuee, message, &block)
        Util.assert_instance_of(::String, valuee, message, &block)
      end

      def new_bigint(value)
        Integer(value)
      end

      def assert_bigint(valuee, message, &block)
        Util.assert_instance_of(::Integer, valuee, message, &block)
      end

      def new_counter(value)
        Integer(value)
      end

      def assert_counter(valuee, message, &block)
        Util.assert_instance_of(::Integer, valuee, message, &block)
      end

      def new_int(value)
        Integer(value)
      end

      def assert_int(valuee, message, &block)
        Util.assert_instance_of(::Integer, valuee, message, &block)
      end

      def new_varint(value)
        Integer(value)
      end

      def assert_varint(valuee, message, &block)
        Util.assert_instance_of(::Integer, valuee, message, &block)
      end

      def new_boolean(value)
        !!value
      end

      def assert_boolean(valuee, message, &block)
        Util.assert_instance_of_one_of([::TrueClass, ::FalseClass], valuee, message, &block)
      end

      def new_decimal(value)
        ::BigDecimal.new(value)
      end

      def assert_decimal(valuee, message, &block)
        Util.assert_instance_of(::BigDecimal, valuee, message, &block)
      end

      def new_double(value)
        Float(value)
      end

      def assert_double(valuee, message, &block)
        Util.assert_instance_of(::Float, valuee, message, &block)
      end

      def new_float(value)
        Float(value)
      end

      def assert_float(valuee, message, &block)
        Util.assert_instance_of(::Float, valuee, message, &block)
      end

      def new_inet(value)
        ::IPAddr.new(value)
      end

      def assert_inet(valuee, message, &block)
        Util.assert_instance_of(::IPAddr, valuee, message, &block)
      end

      def new_timestamp(value)
        case value
        when ::Time
          value
        else
          return value.to_time if value.respond_to?(:to_time)
          raise ::ArgumentError, "cannot convert #{value.inspect} to timestamp"
        end
      end

      def assert_timestamp(valuee, message, &block)
        Util.assert_instance_of(::Time, valuee, message, &block)
      end

      def new_uuid(valuee, message, &block)
        Cassandra::Uuid.new(value)
      end

      def assert_uuid(valuee, message, &block)
        Util.assert_instance_of(Cassandra::Uuid, valuee, message, &block)
      end

      def new_timeuuid(value)
        Cassandra::TimeUuid.new(value)
      end

      def assert_timeuuid(valuee, message, &block)
        Util.assert_instance_of(Cassandra::TimeUuid, valuee, message, &block)
      end
    end

    # @!parse
    #   class Varchar < Type
    #     # @return [Symbol] `:varchar`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to String
    #     # @param value [Object] original value
    #     # @return [String] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is a String
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a String
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"varchar"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Varchar = Simple.new(:varchar)

    # @!parse
    #   class Text < Type
    #     # @return [Symbol] `:text`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to String
    #     # @param value [Object] original value
    #     # @return [String] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is a String
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a String
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"text"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Text = Simple.new(:text)

    # @!parse
    #   class Blob < Type
    #     # @return [Symbol] `:blob`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to String
    #     # @param value [Object] original value
    #     # @return [String] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is a String
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a String
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"blob"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Blob = Simple.new(:blob)

    # @!parse
    #   class Ascii < Type
    #     # @return [Symbol] `:ascii`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to String
    #     # @param value [Object] original value
    #     # @return [String] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is a String
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a String
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"ascii"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Ascii = Simple.new(:ascii)

    # @!parse
    #   class Bigint < Type
    #     # @return [Symbol] `:bigint`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to Integer
    #     # @param value [Object] original value
    #     # @return [Integer] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is an Integer
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not an Integer
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"bigint"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Bigint = Simple.new(:bigint)

    # @!parse
    #   class Counter < Type
    #     # @return [Symbol] `:counter`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to Integer
    #     # @param value [Object] original value
    #     # @return [Integer] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is an Integer
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not an Integer
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"counter"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Counter = Simple.new(:counter)

    # @!parse
    #   class Varchar < Type
    #     # @return [Symbol] `:int`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to Integer
    #     # @param value [Object] original value
    #     # @return [Integer] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is an Integer
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not an Integer
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"int"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Int = Simple.new(:int)

    # @!parse
    #   class Varchar < Type
    #     # @return [Symbol] `:varint`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to Integer
    #     # @param value [Object] original value
    #     # @return [Integer] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is an Integer
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not an Integer
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"varint"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Varint = Simple.new(:varint)

    # @!parse
    #   class Boolean < Type
    #     # @return [Symbol] `:boolean`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to `true` or `false`
    #     # @param value [Object] original value
    #     # @return [Boolean] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is a `true` or `false`
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not `true` or `false`
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"boolean"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Boolean = Simple.new(:boolean)

    # @!parse
    #   class Decimal < Type
    #     # @return [Symbol] `:decimal`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to BigDecimal
    #     # @param value [Object] original value
    #     # @return [BigDecimal] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is a BigDecimal
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a BigDecimal
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"decimal"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Decimal = Simple.new(:decimal)

    # @!parse
    #   class Double < Type
    #     # @return [Symbol] `:double`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to Float
    #     # @param value [Object] original value
    #     # @return [Float] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is a Float
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a Float
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"double"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Double = Simple.new(:double)

    # @!parse
    #   class Float < Type
    #     # @return [Symbol] `:float`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to Float
    #     # @param value [Object] original value
    #     # @return [Float] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is a Float
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a Float
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"float"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Float = Simple.new(:float)

    # @!parse
    #   class Inet < Type
    #     # @return [Symbol] `:inet`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to IPAddr
    #     # @param value [Object] original value
    #     # @return [IPAddr] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is an IPAddr
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not an IPAddr
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"inet"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Inet = Simple.new(:inet)

    # @!parse
    #   class Timestamp < Type
    #     # @return [Symbol] `:timestamp`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to Time
    #     # @param value [Object] original value
    #     # @return [Time] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is a Time
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a Time
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"timestamp"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Timestamp = Simple.new(:timestamp)

    # @!parse
    #   class Uuid < Type
    #     # @return [Symbol] `:uuid`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to Cassandra::Uuid
    #     # @param value [Object] original value
    #     # @return [Cassandra::Uuid] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is a Cassandra::Uuid
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a Cassandra::Uuid
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"uuid"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Uuid = Simple.new(:uuid)

    # @!parse
    #   class Uuid < Type
    #     # @return [Symbol] `:timeuuid`
    #     # @see Cassandra::Type#kind
    #     def self.kind
    #     end
    #
    #     # Coerces the value to Cassandra::Timeuuid
    #     # @param value [Object] original value
    #     # @return [Cassandra::Timeuuid] value
    #     # @see Cassandra::Type#new
    #     def self.new(value)
    #     end
    #
    #     # Asserts that a given value is a Cassandra::Timeuuid
    #     # @param value [Object] value to be validated
    #     # @param message [String] (nil) error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a Cassandra::Timeuuid
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def self.assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"timeuuid"`
    #     # @see Cassandra::Type#to_s
    #     def self.to_s
    #     end
    #   end
    Timeuuid  = Simple.new(:timeuuid)

    class List < Type
      # @private
      attr_reader :value_type

      # @private
      def initialize(value_type)
        @value_type = value_type
      end

      # @return [Symbol] `:list`
      # @see Cassandra::Type#kind
      def kind
        :list
      end

      # Coerces the value to Array
      # @param value [Object] original value
      # @return [Array] value
      # @see Cassandra::Type#new
      def new(*value)
        value = Array(value.first) if value.one?

        value.each do |v|
          Util.assert_type(@value_type, v)
        end
        value
      end

      # Asserts that a given value is an Array
      # @param value [Object] value to be validated
      # @param message [String] (nil) error message to use when assertion fails
      # @yieldreturn [String] error message to use when assertion fails
      # @raise [ArgumentError] if the value is not an Array
      # @return [void]
      # @see Cassandra::Type#assert
      def assert(value, message = nil, &block)
        Util.assert_instance_of(::Array, value, message, &block)
        value.each do |v|
          Util.assert_type(@value_type, v, message, &block)
        end
        nil
      end

      # @return [String] `"list<type>"`
      # @see Cassandra::Type#to_s
      def to_s
        "list<#{@value_type.to_s}>"
      end

      def eql?(other)
        other.is_a?(List) && @value_type == other.value_type
      end
      alias :== :eql?
    end

    class Map < Type
      # @private
      attr_reader :key_type, :value_type

      # @private
      def initialize(key_type, value_type)
        @key_type   = key_type
        @value_type = value_type
      end

      # @return [Symbol] `:map`
      # @see Cassandra::Type#kind
      def kind
        :map
      end

      # Coerces the value to Hash
      # @param value [Object] original value
      # @return [Hash] value
      # @see Cassandra::Type#new
      def new(*value)
        value = value.first if value.one?

        case value
        when ::Hash
          value.each do |k, v|
            Util.assert_type(@key_type, k)
            Util.assert_type(@value_type, v)
          end
          value
        when ::Array
          result = ::Hash.new
          value.each_slice(2) do |(k, v)|
            Util.assert_type(@key_type, k)
            Util.assert_type(@value_type, v)
            result[k] = v
          end
          result
        else
          raise ::ArgumentError, "cannot convert #{value.inspect} to #{to_s}"
        end
      end

      # Asserts that a given value is a Hash
      # @param value [Object] value to be validated
      # @param message [String] (nil) error message to use when assertion fails
      # @yieldreturn [String] error message to use when assertion fails
      # @raise [ArgumentError] if the value is not a Hash
      # @return [void]
      # @see Cassandra::Type#assert
      def assert(value, message = nil, &block)
        Util.assert_instance_of(::Hash, value, message, &block)
        value.each do |k, v|
          Util.assert_type(@key_type, k, message, &block)
          Util.assert_type(@value_type, v, message, &block)
        end
        nil
      end

      # @return [String] `"map<type, type>"`
      # @see Cassandra::Type#to_s
      def to_s
        "map<#{@key_type.to_s}, #{@value_type.to_s}>"
      end

      def eql?(other)
        other.is_a?(Map) &&
          @key_type == other.key_type &&
          @value_type == other.value_type
      end
      alias :== :eql?
    end

    class Set < Type
      # @private
      attr_reader :value_type

      # @private
      def initialize(value_type)
        @value_type = value_type
      end

      # @return [Symbol] `:set`
      # @see Cassandra::Type#kind
      def kind
        :set
      end

      # Coerces the value to Set
      # @param value [Object] original value
      # @return [Set] value
      # @see Cassandra::Type#new
      # @example Creating a set using splat arguments
      #   include Cassandra::Types
      #
      #   set(varchar).new('Jane', 'Alice', 'Loren') => #<Set: {"Jane", "Alice", "Loren"}>
      #
      # @example Coercing an existing set
      #   include Cassandra::Types
      #
      #   set(varchar).new(Set['Jane', 'Alice', 'Loren']) => #<Set: {"Jane", "Alice", "Loren"}>
      #
      # @example Coercing an array
      #   include Cassandra::Types
      #
      #   set(varchar).new(['Jane', 'Alice', 'Loren']) => #<Set: {"Jane", "Alice", "Loren"}>
      def new(*value)
        value = value.first if value.one?

        case value
        when ::Array
          result = ::Set.new
          value.each do |v|
            Util.assert_type(@value_type, v)
            result << v
          end
          result
        when ::Set
          value.each do |v|
            Util.assert_type(@value_type, v)
          end
          value
        else
          Util.assert_type(@value_type, value)
          ::Set[value]
        end
      end

      # Asserts that a given value is an Set
      # @param value [Object] value to be validated
      # @param message [String] (nil) error message to use when assertion fails
      # @yieldreturn [String] error message to use when assertion fails
      # @raise [ArgumentError] if the value is not an Set
      # @return [void]
      # @see Cassandra::Type#assert
      def assert(value, message = nil, &block)
        Util.assert_instance_of(::Set, value, message, &block)
        value.each do |v|
          Util.assert_type(@value_type, v, message, &block)
        end
        nil
      end

      # @return [String] `"set<type>"`
      # @see Cassandra::Type#to_s
      def to_s
        "set<#{@value_type.to_s}>"
      end

      def eql?(other)
        other.is_a?(Set) && @value_type == other.value_type
      end
      alias :== :eql?
    end

    class Tuple < Type
      # @private
      attr_reader :members

      # @private
      def initialize(*members)
        @members = members
      end

      # @return [Symbol] `:tuple`
      # @see Cassandra::Type#kind
      def kind
        :tuple
      end

      # Coerces the value to Cassandra::Tuple
      # @param value [Object] original value
      # @return [Cassandra::Tuple] value
      # @see Cassandra::Type#new
      # @example Creating a tuple
      #   include Cassandra::Types
      #
      #   tuple(varchar, varchar, int).new('Jane', 'Smith', 38) # => (Jane, Smith, 38)
      def new(*values)
        Util.assert(values.size <= @members.size) { "too many values: #{values.size} out of max #{@members.size}" }
        values.each_with_index do |v, i|
          Util.assert_type(@members[i], v)
        end
        Cassandra::Tuple::Strict.new(@members, values)
      end

      # Asserts that a given value is an Cassandra::Tuple
      # @param value [Object] value to be validated
      # @param message [String] (nil) error message to use when assertion fails
      # @yieldreturn [String] error message to use when assertion fails
      # @raise [ArgumentError] if the value is not an Cassandra::Tuple
      # @return [void]
      # @see Cassandra::Type#assert
      def assert(value, message = nil, &block)
        Util.assert_instance_of(Cassandra::Tuple, value, message, &block)
        Util.assert(value.size <= @members.size, message, &block)
        value.zip(@members) do |(v, t)|
          Util.assert_type(t, v, message, &block)
        end
        nil
      end

      # @return [String] `"tuple<type, type, type...>"`
      # @see Cassandra::Type#to_s
      def to_s
        "tuple<#{@members.map(&:to_s).join(', ')}>"
      end

      def eql?(other)
        other.is_a?(Tuple) && @members == other.members
      end
      alias :== :eql?
    end

    class UserDefined < Type
      class Field
        # @return [String] name of this field
        attr_reader :name
        # @return [Cassandra::Type] type of this field
        attr_reader :type

        # @private
        def initialize(name, type)
          @name = name
          @type = type
        end

        # String representation of the field
        # @return [String] String representation of the field
        def to_s
          "#{@name} #{@type}"
        end
      end

      # @return [String] keyspace where this type is defined
      attr_reader :keyspace

      # @return [String] name of this type
      attr_reader :name

      # @private
      attr_reader :fields

      # @private
      def initialize(keyspace, name, fields)
        @keyspace  = keyspace
        @name      = name
        @fields    = fields
      end

      # @param name [String] field name
      # @return [Boolean] whether this type has a given field
      def has_field?(name)
        @fields.any? {|f| f.name == name}
      end

      # Yield or enumerate each field defined in this type
      # @overload each_field
      #   @yieldparam field [Cassandra::UserDefined::Field] field
      #   @return [Cassandra::Types::UserDefined] self
      # @overload each_field
      #   @return [Array<Array<String, Cassandra::Type>>] a list of fields
      def each_field(&block)
        if block_given?
          @fields.each(&block)
          self
        else
          @fields.dup
        end
      end
      alias :fields :each_field

      # @return [Symbol] `:udt`
      # @see Cassandra::Type#kind
      def kind
        :udt
      end

      # Coerces the value to Cassandra::UDT
      # @param value [Object] original value
      # @return [Cassandra::UDT] value
      # @see Cassandra::Type#new
      def new(*value)
        value = value.first if value.one?
        value = Array(value) unless value.is_a?(::Hash)

        Util.assert(value.size <= @fields.size) { "too many values: #{value.size} out of #{@fields.size}" }

        case value
        when ::Array
          result = ::Hash.new
          value.each_with_index do |v, i|
            f = @fields[i]
            Util.assert_type(f.type, v)
            result[f.name] = v
          end
        when ::Hash
          result = ::Hash.new
          @fields.each do |f|
            n = f.name
            v = value[n]
            Util.assert_type(f.type, v)
            result[n] = v
          end
        end

        Cassandra::UDT::Strict.new(@keyspace, @name, @fields, result)
      end

      # Asserts that a given value is an Cassandra::UDT
      # @param value [Object] value to be validated
      # @param message [String] (nil) error message to use when assertion fails
      # @yieldreturn [String] error message to use when assertion fails
      # @raise [ArgumentError] if the value is not an Cassandra::UDT
      # @return [void]
      # @see Cassandra::Type#assert
      def assert(value, message = nil, &block)
        Util.assert_instance_of(Cassandra::UDT, value, message, &block)
        Util.assert(value.size <= @fields.size, message, &block)
        value.zip(@fields) do |((_, v), f)|
          Util.assert_type(f.type, v, message, &block)
        end
        nil
      end

      # @return [String] `"keyspace.name"`
      # @see Cassandra::Type#to_s
      def to_s
        "#{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)} {#{@fields.join(', ')}}"
      end

      def eql?(other)
        other.is_a?(UserDefined) &&
          @keyspace == other.keyspace &&
          @name == other.name &&
          @fields == other.fields
      end
      alias :== :eql?

      # Output this type in CQL
      def to_cql
        cql   = "CREATE TYPE #{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)} (\n"
        first = true

        @fields.each do |field|
          if first
            first = false
          else
            cql << ",\n" unless first
          end
          cql << "  #{field.name} #{type_to_cql(field.type)}"
        end

        cql << "\n);"

        cql
      end

      private

      def type_to_cql(type)
        case type.kind
        when :tuple
          "frozen <#{type}>"
        when :udt
          if @keyspace == type.keyspace
            "frozen <#{Util.escape_name(type.name)}>"
          else
            "frozen <#{Util.escape_name(type.keyspace)}.#{Util.escape_name(type.name)}>"
          end
        else
          "#{type}"
        end
      end
    end

    class Custom < Type
      attr_reader :name

      def initialize(name)
        @name = name
      end

      # @return [Symbol] shorthand type name
      def kind
        :custom
      end

      # Coerces a given value to this type
      #
      # @param value [Object] value to be coerced
      # @return [Object] a value of this type
      def new(*values)
        raise ::NotImplementedError, "unable to create a value for custom type: #{@name.inspect}"
      end

      # Asserts that a given value is of this type
      # @param value [Object] value to be validated
      # @param message [String] (nil) error message to use when assertion fails
      # @yieldreturn [String] error message to use when assertion fails
      # @raise [ArgumentError] if the value is invalid
      # @return [void]
      def assert(value, message = nil, &block)
        raise ::NotImplementedError, "unable to assert a value for custom type: #{@name.inspect}"
      end

      # @return [String] a cassandra representation of this type
      def to_s
        "custom: #{@name}"
      end
    end

    def varchar
      Varchar
    end

    def text
      Text
    end

    def blob
      Blob
    end

    def ascii
      Ascii
    end

    def bigint
      Bigint
    end

    def counter
      Counter
    end

    def int
      Int
    end

    def varint
      Varint
    end

    def boolean
      Boolean
    end

    def decimal
      Decimal
    end

    def double
      Double
    end

    def float
      Float
    end

    def inet
      Inet
    end

    def timestamp
      Timestamp
    end

    def uuid
      Uuid
    end

    def timeuuid
      Timeuuid
    end

    def list(value_type)
      Util.assert_instance_of(Cassandra::Type, value_type,
        "list type must be a Cassandra::Type, #{value_type.inspect} given"
      )

      List.new(value_type)
    end

    def map(key_type, value_type)
      Util.assert_instance_of(Cassandra::Type, key_type,
        "map key type must be a Cassandra::Type, #{key_type.inspect} given"
      )
      Util.assert_instance_of(Cassandra::Type, value_type,
        "map value type must be a Cassandra::Type, #{value_type.inspect} given"
      )

      Map.new(key_type, value_type)
    end

    def set(value_type)
      Util.assert_instance_of(Cassandra::Type, value_type,
        "set type must be a Cassandra::Type, #{value_type.inspect} given"
      )

      Set.new(value_type)
    end

    def tuple(*members)
      Util.assert_not_empty(members, "tuple must contain at least one member")
      members.each do |member|
        Util.assert_instance_of(Cassandra::Type, member,
          "each tuple member must be a Cassandra::Type, " \
          "#{member.inspect} given"
        )
      end

      Tuple.new(*members)
    end

    # Creates a User Defined Type instance
    # @example Various ways of defining the same UDT
    #   include Cassandra::Types
    #
    #   udt('simplex', 'address', {'street' => varchar, 'city' => varchar, 'state' => varchar, 'zip' => varchar}) #=> simplex.address
    #
    #   udt('simplex', 'address', [['street', varchar], ['city', varchar], ['state', varchar], ['zip', varchar]]) #=> simplex.address
    #
    #   udt('simplex', 'address', ['street', varchar], ['city', varchar], ['state', varchar], ['zip', varchar]) #=> simplex.address
    #
    #   udt('simplex', 'address', 'street', varchar, 'city', varchar, 'state', varchar, 'zip', varchar) #=> simplex.address
    def udt(keyspace, table, *fields)
      keyspace = String(keyspace)
      table    = String(table)
      fields   = Array(fields.first) if fields.one?

      Util.assert_not_empty(fields,
        "user-defined type must contain at least one field"
      )

      if fields.first.is_a?(::Array)
        fields = fields.map do |pair|
          Util.assert(pair.size == 2,
            "fields of a user-defined type must be an Array of name and " \
            "value pairs, #{pair.inspect} given"
          )
          Util.assert_instance_of(::String, pair[0],
            "each field name for a user-defined type must be a String, " \
            "#{pair[0].inspect} given"
          )
          Util.assert_instance_of(Cassandra::Type, pair[1],
            "each field type for a user-defined type must be a " \
            "Cassandra::Type, #{pair[1].inspect} given"
          )

          UserDefined::Field.new(*pair)
        end
      else
        Util.assert((fields.size % 2) == 0,
          "fields of a user-defined type must be an Array of alternating " \
          "names and values pairs, #{fields.inspect} given"
        )
        fields = fields.each_slice(2).map do |name, type|
          Util.assert_instance_of(::String, name,
            "each field name for a user-defined type must be a String, " \
            "#{name.inspect} given"
          )
          Util.assert_instance_of(Cassandra::Type, type,
            "each field type for a user-defined type must be a " \
            "Cassandra::Type, #{type.inspect} given"
          )

          UserDefined::Field.new(name, type)
        end
      end

      UserDefined.new(keyspace, table, fields)
    end

    def custom(name)
      Custom.new(name)
    end
  end
end
