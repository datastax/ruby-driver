# encoding: utf-8

#--
# Copyright DataStax, Inc.
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
    attr_reader :kind

    def initialize(kind)
      @kind = kind
    end

    # Coerces a given value to this type
    #
    # @param values [*Object] value to be coerced
    # @return [Object] a value of this type
    def new(*values)
    end

    # Asserts that a given value is of this type
    # @param value [Object] value to be validated
    # @param message [String] error message to use when assertion fails
    # @yieldreturn [String] error message to use when assertion fails
    # @raise [ArgumentError] if the value is invalid
    # @return [void]
    def assert(value, message = nil, &block)
    end

    # @return [String] a cassandra representation of this type
    def to_s
    end
  end

  module Types
    # If we use module_function, the yard docs end up showing duplicates of all
    # methods: one for self, the other as instance methods.
    #
    # rubocop:disable Style/ModuleFunction
    extend self

    # @private
    class Simple < Type
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

      def hash
        @hash ||= 31 * 17 + @kind.hash
      end

      def eql?(other)
        other.is_a?(Simple) && @kind == other.kind
      end

      alias == eql?

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

      def assert_text(value, message, &block)
        Util.assert_instance_of(::String, value, message, &block)
      end

      def new_blob(value)
        String(value)
      end

      def assert_blob(value, message, &block)
        Util.assert_instance_of(::String, value, message, &block)
      end

      def new_ascii(value)
        String(value)
      end

      def assert_ascii(value, message, &block)
        Util.assert_instance_of(::String, value, message, &block)
      end

      def new_bigint(value)
        Integer(value)
      end

      def assert_bigint(value, message, &block)
        Util.assert_instance_of(::Integer, value, message, &block)
      end

      def new_counter(value)
        Integer(value)
      end

      def assert_counter(value, message, &block)
        Util.assert_instance_of(::Integer, value, message, &block)
      end

      def new_int(value)
        Integer(value)
      end

      def assert_int(value, message, &block)
        Util.assert_instance_of(::Integer, value, message, &block)
      end

      def new_varint(value)
        Integer(value)
      end

      def assert_varint(value, message, &block)
        Util.assert_instance_of(::Integer, value, message, &block)
      end

      def new_boolean(value)
        !value.nil? && value != false
      end

      def assert_boolean(value, message, &block)
        Util.assert_instance_of_one_of([::TrueClass, ::FalseClass],
                                       value,
                                       message,
                                       &block)
      end

      def new_decimal(value)
        BigDecimal(value)
      end

      def assert_decimal(value, message, &block)
        Util.assert_instance_of(::BigDecimal, value, message, &block)
      end

      def new_double(value)
        Float(value)
      end

      def assert_double(value, message, &block)
        Util.assert_instance_of(::Float, value, message, &block)
      end

      def new_float(value)
        Float(value)
      end

      def assert_float(value, message, &block)
        Util.assert_instance_of(::Float, value, message, &block)
      end

      def new_inet(value)
        ::IPAddr.new(value)
      end

      def assert_inet(value, message, &block)
        Util.assert_instance_of(::IPAddr, value, message, &block)
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

      def assert_timestamp(value, message, &block)
        Util.assert_instance_of(::Time, value, message, &block)
      end

      def new_uuid(value)
        Cassandra::Uuid.new(value)
      end

      def assert_uuid(value, message, &block)
        Util.assert_instance_of(Cassandra::Uuid, value, message, &block)
      end

      def new_timeuuid(value)
        Cassandra::TimeUuid.new(value)
      end

      def assert_timeuuid(value, message, &block)
        Util.assert_instance_of(Cassandra::TimeUuid, value, message, &block)
      end

      def new_date(value)
        case value
        when ::Date
          value
        else
          return value.to_date if value.respond_to?(:to_date)
          raise ::ArgumentError, "cannot convert #{value.inspect} to date"
        end
      end

      def assert_date(value, message, &block)
        Util.assert_instance_of(::Date, value, message, &block)
      end

      def new_smallint(value)
        Integer(value)
      end

      def assert_smallint(value, message, &block)
        Util.assert_instance_of(::Integer, value, message, &block)
        Util.assert(value <= 32767 && value >= -32768, message, &block)
      end

      def new_time(value)
        case value
        when ::Integer
          Time.new(value)
        else
          return Time.new(value.to_nanoseconds) if value.respond_to?(:to_nanoseconds)
          raise ::ArgumentError, "cannot convert #{value.inspect} to time"
        end
      end

      def assert_time(value, message, &block)
        Util.assert_instance_of(Cassandra::Time, value, message, &block)
      end

      def new_tinyint(value)
        Integer(value)
      end

      def assert_tinyint(value, message, &block)
        Util.assert_instance_of(::Integer, value, message, &block)
        Util.assert(value <= 127 && value >= -128, message, &block)
      end
    end

    # @!parse
    #   class Ascii < Type
    #     # @return [Symbol] `:ascii`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to String
    #     # @param value [Object] original value
    #     # @return [String] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is a String
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a String
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"ascii"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Ascii', Simple.new(:ascii))

    # @!parse
    #   class Bigint < Type
    #     # @return [Symbol] `:bigint`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to Integer
    #     # @param value [Object] original value
    #     # @return [Integer] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is an Integer
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not an Integer
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"bigint"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Bigint', Simple.new(:bigint))

    # @!parse
    #   class Blob < Type
    #     # @return [Symbol] `:blob`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to String
    #     # @param value [Object] original value
    #     # @return [String] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is a String
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a String
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"blob"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Blob', Simple.new(:blob))

    # @!parse
    #   class Boolean < Type
    #     # @return [Symbol] `:boolean`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to `true` or `false`
    #     # @param value [Object] original value
    #     # @return [Boolean] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is a `true` or `false`
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not `true` or `false`
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"boolean"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Boolean', Simple.new(:boolean))

    # @!parse
    #   class Counter < Type
    #     # @return [Symbol] `:counter`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to Integer
    #     # @param value [Object] original value
    #     # @return [Integer] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is an Integer
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not an Integer
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"counter"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Counter', Simple.new(:counter))

    # @!parse
    #   class Date < Type
    #     # @return [Symbol] `:date`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to Date
    #     # @param value [Object] original value
    #     # @return [Date] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is a Date
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a Date
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"date"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Date', Simple.new(:date))

    # @!parse
    #   class Decimal < Type
    #     # @return [Symbol] `:decimal`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to BigDecimal
    #     # @param value [Object] original value
    #     # @return [BigDecimal] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is a BigDecimal
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a BigDecimal
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"decimal"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Decimal', Simple.new(:decimal))

    # @!parse
    #   class Double < Type
    #     # @return [Symbol] `:double`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to Float
    #     # @param value [Object] original value
    #     # @return [Float] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is a Float
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a Float
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"double"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Double', Simple.new(:double))

    # @!parse
    #   class Float < Type
    #     # @return [Symbol] `:float`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to Float
    #     # @param value [Object] original value
    #     # @return [Float] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is a Float
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a Float
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"float"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Float', Simple.new(:float))

    # @!parse
    #   class Inet < Type
    #     # @return [Symbol] `:inet`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to IPAddr
    #     # @param value [Object] original value
    #     # @return [IPAddr] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is an IPAddr
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not an IPAddr
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"inet"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Inet', Simple.new(:inet))

    # @!parse
    #   class Int < Type
    #     # @return [Symbol] `:int`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to Integer
    #     # @param value [Object] original value
    #     # @return [Integer] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is an Integer
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not an Integer
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"int"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Int', Simple.new(:int))

    class List < Type
      # @private
      attr_reader :value_type

      # @private
      def initialize(value_type)
        super(:list)
        @value_type = value_type
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
      # @param message [String] error message to use when assertion fails
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
        "list<#{@value_type}>"
      end

      def hash
        @hash ||= begin
          h = 17
          h = 31 * h + @kind.hash
          h = 31 * h + @value_type.hash
          h
        end
      end

      def eql?(other)
        other.is_a?(List) && @value_type == other.value_type
      end

      alias == eql?
    end

    class Map < Type
      # @private
      attr_reader :key_type, :value_type

      # @private
      def initialize(key_type, value_type)
        super(:map)
        @key_type = key_type
        @value_type = value_type
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
          raise ::ArgumentError, "cannot convert #{value.inspect} to #{self}"
        end
      end

      # Asserts that a given value is a Hash
      # @param value [Object] value to be validated
      # @param message [String] error message to use when assertion fails
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
        "map<#{@key_type}, #{@value_type}>"
      end

      def hash
        @hash ||= begin
          h = 17
          h = 31 * h + @kind.hash
          h = 31 * h + @key_type.hash
          h = 31 * h + @value_type.hash
          h
        end
      end

      def eql?(other)
        other.is_a?(Map) &&
          @key_type == other.key_type &&
          @value_type == other.value_type
      end

      alias == eql?
    end

    class Set < Type
      # @private
      attr_reader :value_type

      # @private
      def initialize(value_type)
        super(:set)
        @value_type = value_type
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
      # @param message [String] error message to use when assertion fails
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
        "set<#{@value_type}>"
      end

      def hash
        @hash ||= begin
          h = 17
          h = 31 * h + @kind.hash
          h = 31 * h + @value_type.hash
          h
        end
      end

      def eql?(other)
        other.is_a?(Set) && @value_type == other.value_type
      end

      alias == eql?
    end

    # @!parse
    #   class Smallint < Type
    #     # @return [Symbol] `:smallint`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to an Integer
    #     # @param value [Object] original value
    #     # @return [Integer] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is an Integer
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not an Integer
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"smallint"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Smallint', Simple.new(:smallint))

    # @!parse
    #   class Text < Type
    #     # @return [Symbol] `:text`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to String
    #     # @param value [Object] original value
    #     # @return [String] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is a String
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a String
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"text"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Text', Simple.new(:text))

    # @!parse
    #   class Time < Type
    #     # @return [Symbol] `:time`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to Time
    #     # @param value [Object] original value
    #     # @return [Time] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is a Time
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a String
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"time"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Time', Simple.new(:time))

    # @!parse
    #   class Timestamp < Type
    #     # @return [Symbol] `:timestamp`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to Time
    #     # @param value [Object] original value
    #     # @return [Time] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is a Time
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a Time
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"timestamp"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Timestamp', Simple.new(:timestamp))

    # @!parse
    #   class Timeuuid < Type
    #     # @return [Symbol] `:timeuuid`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to Cassandra::Timeuuid
    #     # @param value [Object] original value
    #     # @return [Cassandra::Timeuuid] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is a Cassandra::Timeuuid
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a Cassandra::Timeuuid
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"timeuuid"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Timeuuid', Simple.new(:timeuuid))

    # @!parse
    #   class Tinyint < Type
    #     # @return [Symbol] `:tinyint`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to Integer
    #     # @param value [Object] original value
    #     # @return [Integer] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is an Integer
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not an Integer
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"tinyint"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Tinyint', Simple.new(:tinyint))

    class Tuple < Type
      # @private
      attr_reader :members

      # @private
      def initialize(*members)
        super(:tuple)
        @members = members
      end

      # Coerces the value to Cassandra::Tuple
      # @param values [*Object] tuple values
      # @return [Cassandra::Tuple] value
      # @see Cassandra::Type#new
      # @example Creating a tuple
      #   include Cassandra::Types
      #
      #   tuple(varchar, varchar, int).new('Jane', 'Smith', 38) # => (Jane, Smith, 38)
      def new(*values)
        Util.assert(values.size <= @members.size) do
          "too many values: #{values.size} out of max #{@members.size}"
        end
        values.each_with_index do |v, i|
          Util.assert_type(@members[i], v)
        end
        Cassandra::Tuple::Strict.new(@members, values)
      end

      # Asserts that a given value is an Cassandra::Tuple
      # @param value [Object] value to be validated
      # @param message [String] error message to use when assertion fails
      # @yieldreturn [String] error message to use when assertion fails
      # @raise [ArgumentError] if the value is not an Cassandra::Tuple
      # @return [void]
      # @see Cassandra::Type#assert
      def assert(value, message = nil, &block)
        Util.assert_instance_of(Cassandra::Tuple, value, message, &block)
        Util.assert(value.size <= @members.size, message, &block)
        @members.each_with_index do |type, i|
          Util.assert_type(type, value[i], message, &block)
        end
        nil
      end

      # @return [String] `"tuple<type, type, type...>"`
      # @see Cassandra::Type#to_s
      def to_s
        "tuple<#{@members.map(&:to_s).join(', ')}>"
      end

      def hash
        @hash ||= begin
          h = 17
          h = 31 * h + @kind.hash
          h = 31 * h + @members.hash
          h
        end
      end

      def eql?(other)
        other.is_a?(Tuple) && @members == other.members
      end

      alias == eql?
    end

    # @!parse
    #   class Uuid < Type
    #     # @return [Symbol] `:uuid`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to Cassandra::Uuid
    #     # @param value [Object] original value
    #     # @return [Cassandra::Uuid] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is a Cassandra::Uuid
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not a Cassandra::Uuid
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"uuid"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Uuid', Simple.new(:uuid))

    # @!parse
    #   class Varint < Type
    #     # @return [Symbol] `:varint`
    #     # @see Cassandra::Type#kind
    #     def kind
    #     end
    #
    #     # Coerces the value to Integer
    #     # @param value [Object] original value
    #     # @return [Integer] value
    #     # @see Cassandra::Type#new
    #     def new(value)
    #     end
    #
    #     # Asserts that a given value is an Integer
    #     # @param value [Object] value to be validated
    #     # @param message [String] error message to use when assertion
    #     #   fails
    #     # @yieldreturn [String] error message to use when assertion fails
    #     # @raise [ArgumentError] if the value is not an Integer
    #     # @return [void]
    #     # @see Cassandra::Type#assert
    #     def assert(value, message = nil, &block)
    #     end
    #
    #     # @return [String] `"varint"`
    #     # @see Cassandra::Type#to_s
    #     def to_s
    #     end
    #   end
    const_set('Varint', Simple.new(:varint))

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

        def hash
          @hash ||= begin
            h = 17
            h = 31 * h + @name.hash
            h = 31 * h + @type.hash
            h
          end
        end

        def eql?(other)
          other.is_a?(Field) &&
            @name == other.name &&
            @type == other.type
        end

        alias == eql?
      end

      # @return [String] keyspace where this type is defined
      attr_reader :keyspace

      # @return [String] name of this type
      attr_reader :name

      # @private
      attr_reader :fields

      # @private
      def initialize(keyspace, name, fields)
        super(:udt)
        @keyspace = keyspace
        @name = name
        @fields = fields
      end

      # @param name [String] field name
      # @return [Boolean] whether this type has a given field
      def has_field?(name)
        @fields.any? { |f| f.name == name }
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

      alias fields each_field

      # @param name [String] field name
      # @return [Cassandra::UserDefined::Field, nil] a field with this name or
      #   nil
      def field(name)
        @fields.find { |f| f.name == name }
      end

      # Coerces the value to Cassandra::UDT
      # @param value [Object] original value
      # @return [Cassandra::UDT] value
      # @see Cassandra::Type#new
      def new(*value)
        value = value.first if value.one?
        value = Array(value) unless value.is_a?(::Hash)

        Util.assert(value.size <= @fields.size) do
          "too many values: #{value.size} out of #{@fields.size}"
        end

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
      # @param message [String] error message to use when assertion fails
      # @yieldreturn [String] error message to use when assertion fails
      # @raise [ArgumentError] if the value is not an Cassandra::UDT
      # @return [void]
      # @see Cassandra::Type#assert
      def assert(value, message = nil, &block)
        Util.assert_instance_of(Cassandra::UDT, value, message, &block)
        Util.assert(value.size <= @fields.size, message, &block)
        @fields.each do |field|
          Util.assert_type(field.type, value[field.name], message, &block)
        end
        nil
      end

      # @return [String] `"keyspace.name"`
      # @see Cassandra::Type#to_s
      def to_s
        "#{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)} " \
                        "{#{@fields.join(', ')}}"
      end

      def hash
        @hash ||= begin
          h = 17
          h = 31 * h + @kind.hash
          h = 31 * h + @keyspace.hash
          h = 31 * h + @name.hash
          h = 31 * h + @fields.hash
          h
        end
      end

      def eql?(other)
        other.is_a?(UserDefined) &&
          @keyspace == other.keyspace &&
          @name == other.name &&
          @fields == other.fields
      end

      alias == eql?

      # Output this type in CQL
      def to_cql
        cql = "CREATE TYPE #{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)} " \
                        "(\n"
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

      # @private
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
          type.to_s
        end
      end
    end

    class Custom < Type
      attr_reader :name

      def initialize(name)
        super(:custom)
        @name = name
      end

      # Coerces a given value to this type
      #
      # @param value [*Object] value to be coerced
      # @return [Object] a value of this type
      def new(*value)
        raise ::NotImplementedError,
              "unable to create a value for custom type: #{@name.inspect}"
      end

      # Asserts that a given value is of this type
      # @param value [Object] value to be validated
      # @param message [String] error message to use when assertion fails
      # @yieldreturn [String] error message to use when assertion fails
      # @raise [ArgumentError] if the value is invalid
      # @return [void]
      def assert(value, message = nil, &block)
        Util.assert_instance_of(CustomData, value, message, &block)
        Util.assert_equal(self, value.class.type, message, &block)
      end

      # @return [String] a cassandra representation of this type
      def to_s
        "'#{@name}'"
      end

      def hash
        @hash ||= begin
          h = 17
          h = 31 * h + @kind.hash
          h = 31 * h + @name.hash
          h
        end
      end

      def eql?(other)
        other.is_a?(Custom) && @name == other.name
      end

      alias == eql?
    end

    class Frozen < Type
      # @private
      attr_reader :value_type

      # @private
      def initialize(value_type)
        super(:frozen)
        @value_type = value_type
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
      # @param message [String] error message to use when assertion fails
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
        "frozen<#{@value_type}>"
      end

      def hash
        @hash ||= begin
          h = 17
          h = 31 * h + @kind.hash
          h = 31 * h + @value_type.hash
          h
        end
      end

      def eql?(other)
        other.is_a?(List) && @value_type == other.value_type
      end

      alias == eql?
    end

    def frozen(value_type)
      Util.assert_instance_of(Cassandra::Type, value_type,
                              "frozen type must be a Cassandra::Type, #{value_type.inspect} given")

      Frozen.new(value_type)
    end

    # @return [Cassandra::Types::Text] text type since varchar is an alias for text
    def varchar
      Text
    end

    # @return [Cassandra::Types::Text] text type
    def text
      Text
    end

    # @return [Cassandra::Types::Blob] blob type
    def blob
      Blob
    end

    # @return [Cassandra::Types::Ascii] ascii type
    def ascii
      Ascii
    end

    # @return [Cassandra::Types::Bigint] bigint type
    def bigint
      Bigint
    end

    # @return [Cassandra::Types::Counter] counter type
    def counter
      Counter
    end

    # @return [Cassandra::Types::Int] int type
    def int
      Int
    end

    # @return [Cassandra::Types::Varint] varint type
    def varint
      Varint
    end

    # @return [Cassandra::Types::Boolean] boolean type
    def boolean
      Boolean
    end

    # @return [Cassandra::Types::Decimal] decimal type
    def decimal
      Decimal
    end

    # @return [Cassandra::Types::Double] double type
    def double
      Double
    end

    # @return [Cassandra::Types::Float] float type
    def float
      Float
    end

    # @return [Cassandra::Types::Inet] inet type
    def inet
      Inet
    end

    # @return [Cassandra::Types::Timestamp] timestamp type
    def timestamp
      Timestamp
    end

    # @return [Cassandra::Types::Uuid] uuid type
    def uuid
      Uuid
    end

    # @return [Cassandra::Types::Timeuuid] timeuuid type
    def timeuuid
      Timeuuid
    end

    # @return [Cassandra::Types::Date] date type
    def date
      Date
    end

    # @return [Cassandra::Types::Time] time type
    def time
      Time
    end

    # @return [Cassandra::Types::Smallint] smallint type
    def smallint
      Smallint
    end

    # @return [Cassandra::Types::Tinyint] tinyint type
    def tinyint
      Tinyint
    end

    # @param value_type [Cassandra::Type] the type of elements in this list
    # @return [Cassandra::Types::List] list type
    def list(value_type)
      Util.assert_instance_of(Cassandra::Type, value_type,
                              "list type must be a Cassandra::Type, #{value_type.inspect} given")

      List.new(value_type)
    end

    # @param key_type [Cassandra::Type] the type of keys in this map
    # @param value_type [Cassandra::Type] the type of values in this map
    # @return [Cassandra::Types::Map] map type
    def map(key_type, value_type)
      Util.assert_instance_of(Cassandra::Type, key_type,
                              "map key type must be a Cassandra::Type, #{key_type.inspect} given")
      Util.assert_instance_of(Cassandra::Type, value_type,
                              "map value type must be a Cassandra::Type, #{value_type.inspect} given")

      Map.new(key_type, value_type)
    end

    # @param value_type [Cassandra::Type] the type of values in this set
    # @return [Cassandra::Types::Set] set type
    def set(value_type)
      Util.assert_instance_of(Cassandra::Type, value_type,
                              "set type must be a Cassandra::Type, #{value_type.inspect} given")

      Set.new(value_type)
    end

    # @param members [*Cassandra::Type] types of members of this tuple
    # @return [Cassandra::Types::Tuple] tuple type
    def tuple(*members)
      Util.assert_not_empty(members, 'tuple must contain at least one member')
      members.each do |member|
        Util.assert_instance_of(Cassandra::Type, member,
                                'each tuple member must be a Cassandra::Type, ' \
                                            "#{member.inspect} given")
      end

      Tuple.new(*members)
    end

    # Creates a User Defined Type instance
    # @example Various ways of defining the same UDT
    #   include Cassandra::Types
    #
    #   udt('simplex', 'address', {'street' => varchar,
    #                              'city' => varchar,
    #                              'state' => varchar,
    #                              'zip' => varchar}) #=> simplex.address
    #
    #   udt('simplex', 'address', [['street', varchar],
    #                              ['city', varchar],
    #                              ['state', varchar],
    #                              ['zip', varchar]]) #=> simplex.address
    #
    #   udt('simplex', 'address', ['street', varchar],
    #                             ['city', varchar],
    #                             ['state', varchar],
    #                             ['zip', varchar]) #=> simplex.address
    #
    #   udt('simplex', 'address', 'street', varchar,
    #                             'city', varchar,
    #                             'state', varchar,
    #                             'zip', varchar) #=> simplex.address
    # @param keyspace [String] name of the keyspace that this UDT is defined in
    # @param name     [String] name of this UDT
    # @param fields   [Hash<String, Cassandra::Type>,
    #                  Array<Array<String, Cassandra::Type>>,
    #                  *(String, Cassandra::Type),
    #                  *Array<String, Cassandra::Type>] UDT field types
    # @return [Cassandra::Types::UserDefined] user defined type
    def udt(keyspace, name, *fields)
      keyspace = String(keyspace)
      name = String(name)
      fields = Array(fields.first) if fields.one?

      Util.assert_not_empty(fields,
                            'user-defined type must contain at least one field')

      if fields.first.is_a?(::Array)
        fields = fields.map do |pair|
          Util.assert(pair.size == 2,
                      'fields of a user-defined type must be an Array of name and ' \
                                  "value pairs, #{pair.inspect} given")
          Util.assert_instance_of(::String, pair[0],
                                  'each field name for a user-defined type must be a String, ' \
                                              "#{pair[0].inspect} given")
          Util.assert_instance_of(Cassandra::Type, pair[1],
                                  'each field type for a user-defined type must be a ' \
                                              "Cassandra::Type, #{pair[1].inspect} given")

          UserDefined::Field.new(*pair)
        end
      else
        Util.assert(fields.size.even?,
                    'fields of a user-defined type must be an Array of alternating ' \
                                "names and values pairs, #{fields.inspect} given")
        fields = fields.each_slice(2).map do |field_name, field_type|
          Util.assert_instance_of(::String, field_name,
                                  'each field name for a user-defined type must be a String, ' \
                                              "#{field_name.inspect} given")
          Util.assert_instance_of(Cassandra::Type, field_type,
                                  'each field type for a user-defined type must be a ' \
                                              "Cassandra::Type, #{field_type.inspect} given")

          UserDefined::Field.new(field_name, field_type)
        end
      end

      UserDefined.new(keyspace, name, fields)
    end

    # @param name [String] name of the custom type
    # @return [Cassandra::Types::Custom] custom type
    def custom(name)
      Custom.new(name)
    end

    def duration
      Duration.new 0,0,0
    end
  end
end
