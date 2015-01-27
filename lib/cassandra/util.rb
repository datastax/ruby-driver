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
  # @private
  module Util extend self
    def encode_hash(hash, io = StringIO.new)
      first = true

      io.putc(CRL_OPN)
      hash.each do |k, v|
        if first
          first = false
        else
          io.print(COMMA)
        end

        encode_object(k, io)
        io.print(COLON)
        encode_object(v, io)
      end
      io.putc(CRL_CLS)

      io.string
    end

    def encode_set(set, io = StringIO.new)
      first = true

      io.putc(CRL_OPN)
      set.each do |object|
        if first
          first = false
        else
          io.print(COMMA)
        end

        encode_object(object, io)
      end
      io.putc(CRL_CLS)

      io.string
    end

    def encode_array(array, io = StringIO.new)
      first = true

      io.putc(SQR_OPN)
      array.each do |object|
        if first
          first = false
        else
          io.print(COMMA)
        end

        encode_object(object, io)
      end
      io.putc(SQR_CLS)

      io.string
    end

    def encode_string(string, io = StringIO.new)
      io.putc(QUOT)
      string.chars do |c|
        case c
        when QUOT then io.print(ESC_QUOT)
        else
          io.putc(c)
        end
      end
      io.putc(QUOT)

      io.string
    end

    def encode_object(object, io = StringIO.new)
      case object
      when ::Hash    then encode_hash(object, io)
      when ::Array   then encode_array(object, io)
      when ::Set     then encode_set(object, io)
      when ::String  then encode_string(object, io)
      when ::Time    then encode_timestamp(object, io)
      when ::Numeric then encode_number(object, io)
      when ::IPAddr  then encode_inet(object, io)
      when Uuid      then encode_uuid(object, io)
      when nil       then io.print(NULL_STR)
      when false     then io.print(FALSE_STR)
      when true      then io.print(TRUE_STR)
      else
        raise ::ArgumentError, "unsupported type: #{object.inspect}"
      end

      io.string
    end
    alias :encode :encode_object

    def encode_timestamp(time, io = StringIO.new)
      io.print(time.to_i)
      io.string
    end

    def encode_number(number, io = StringIO.new)
      io.print(number)
      io.string
    end

    def encode_uuid(uuid, io = StringIO.new)
      io.print(uuid)
      io.string
    end

    def encode_inet(inet, io = StringIO.new)
      io.putc(QUOT)
      io.print(inet)
      io.putc(QUOT)
      io.string
    end

    def type_to_cql(type)
      case type
      when Array
        case type[0]
        when :list, :set
          "#{type[0].to_s}<#{type_to_cql(type[1])}>"
        when :map
          "#{type[0].to_s}<#{type_to_cql(type[1])}, #{type_to_cql(type[2])}>"
        when :udt
          "frozen <#{escape_name(type[2])}>"
        when :tuple
          "frozen <tuple<#{type[1].map(&method(:type_to_cql)).join(', ')}>>"
        else
          type.to_s
        end
      else
        type.to_s
      end
    end

    def escape_name(name)
      return name if name[LOWERCASE_REGEXP] == name
      DBL_QUOT + name + DBL_QUOT
    end

    def assert_type(type, value, message = nil, &block)
      return if value.nil?

      case type
      when ::Array
        case type.first
        when :list
          assert_instance_of(::Array, value, message, &block)
          value.each do |v|
            assert_type(type[1], v)
          end
        when :set
          assert_instance_of(::Set, value, message, &block)
          value.each do |v|
            assert_type(type[1], v)
          end
        when :map
          assert_instance_of(::Hash, value, message, &block)
          value.each do |k, v|
            assert_type(type[1], k)
            assert_type(type[2], v)
          end
        when :udt
          keyspace = type[1]
          name     = type[2]
          fields   = type[3]

          fields.each do |(field_name, field_type)|
            assert_responds_to(field_name, value, message, &block)
            assert_type(field_type, value.send(field_name))
          end
        when :tuple
          assert_instance_of(::Array, value, message, &block)
          values.zip(type[1]) do |(v, t)|
            assert_type(t, v)
          end
        when :custom
          assert_responds_to_all([:bytesize, :to_s], value, message, &block)
        else
          raise ::RuntimeError, "unsupported complex type #{type.inspect}"
        end
      else
        case type
        when :ascii then assert_instance_of(::String, value, message, &block)
        when :bigint then assert_instance_of(::Numeric, value, message, &block)
        when :blob then assert_instance_of(::String, value, message, &block)
        when :boolean then assert_instance_of_one_of([::TrueClass, ::FalseClass], value, message, &block)
        when :counter then assert_instance_of(::Numeric, value, message, &block)
        when :decimal then assert_instance_of(::BigDecimal, value, message, &block)
        when :double then assert_instance_of(::Float, value, message, &block)
        when :float then assert_instance_of(::Float, value, message, &block)
        when :inet then assert_instance_of(::IPAddr, value, message, &block)
        when :int then assert_instance_of(::Numeric, value, message, &block)
        when :text then assert_instance_of(::String, value, message, &block)
        when :varchar then assert_instance_of(::String, value, message, &block)
        when :timestamp then assert_instance_of(::Time, value, message, &block)
        when :timeuuid then assert_instance_of(TimeUuid, value, message, &block)
        when :uuid then assert_instance_of(Uuid, value, message, &block)
        when :varint then assert_instance_of(::Numeric, value, message, &block)
        else
          raise ::RuntimeError, "unsupported type #{type.inspect}"
        end
      end
    end

    def assert_instance_of(kind, value, message = nil, &block)
      unless value.is_a?(kind)
        message   = yield if block_given?
        message ||= "value must be an instance of #{kind}, #{value.inspect} given"

        raise ::ArgumentError, message
      end
    end

    def assert_instance_of_one_of(kinds, value, message = nil, &block)
      unless kinds.any? {|kind| value.is_a?(kind)}
        message   = yield if block_given?
        message ||= "value must be an instance of one of #{kinds.inspect}, #{value.inspect} given"

        raise ::ArgumentError, message
      end
    end

    def assert_responds_to(method, value, message = nil, &block)
      unless value.respond_to?(method)
        message   = yield if block_given?
        message ||= "value #{value.inspect} must respond to #{method.inspect}, but doesn't"

        raise ::ArgumentError, message
      end
    end

    def assert_responds_to_all(methods, value, message = nil, &block)
      unless methods.all? {|method| value.respond_to?(method)}
        message   = yield if block_given?
        message ||= "value #{value.inspect} must respond to all methods #{methods.inspect}, but doesn't"

        raise ::ArgumentError, message
      end
    end

    def assert_not_empty(value, message = nil, &block)
      if value.empty?
        message   = yield if block_given?
        message ||= "value cannot be empty"

        raise ::ArgumentError, message
      end
    end

    def assert_file_exists(path, message = nil, &block)
      unless ::File.exists?(path)
        message   = yield if block_given?
        message ||= "expected file at #{path.inspect} to exist, but it doesn't"

        raise ::ArgumentError, message
      end
    end

    def assert_one_of(range, value, message = nil, &block)
      unless range.include?(value)
        message   = yield if block_given?
        message ||= "value must be included in #{value.inspect}, #{value.inspect} given"

        raise ::ArgumentError, message
      end
    end

    def assert(condition, message = nil, &block)
      unless condition
        message   = yield if block_given?
        message ||= "assertion failed"

        raise ::ArgumentError, message
      end
    end

    def assert_equal(expected, actual, message = nil, &block)
      unless expected == actual
        message   = yield if block_given?
        message ||= "expected #{actual.inspect} to equal #{expected.inspect}"

        raise ::ArgumentError, message
      end
    end

    # @private
    LOWERCASE_REGEXP = /[[:lower:]\_]*/
    # @private
    NULL_STR = 'null'.freeze
    # @private
    FALSE_STR = 'false'.freeze
    # @private
    TRUE_STR = 'true'.freeze
    # @private
    CRL_OPN = '{'.freeze
    # @private
    CRL_CLS = '}'.freeze
    # @private
    SQR_OPN = '['.freeze
    # @private
    SQR_CLS = ']'.freeze
    # @private
    COMMA = ', '.freeze
    # @private
    COLON = ' : '.freeze
    # @private
    QUOT = ?'.freeze
    # @private
    ESC_QUOT = "''".freeze
    # @private
    DBL_QUOT = ?".freeze
  end
end
