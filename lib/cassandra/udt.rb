# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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
  # A user-defined type value representation
  class UDT
    # @private
    class Strict < UDT
      def initialize(keyspace, name, fields, values)
        @keyspace     = keyspace
        @name         = name
        @fields       = fields
        @values       = values
        @name_to_type = fields.each_with_object(::Hash.new) do |f, index|
          index[f.name] = f.type
        end
      end

      def method_missing(method, *args, &block)
        return super if block_given? || args.size > 1

        field  = method.to_s
        assign = !field.chomp!('=').nil?

        return super if assign && args.empty?
        return super unless @name_to_type.key?(field)

        if assign
          value = args.first
          Util.assert_type(@name_to_type[field], value)
          @values[field] = value
        else
          @values[field]
        end
      end

      # Returns true if a field with a given name is present in this value
      #
      # @param method [Symbol] name of the field
      #
      # @return [Boolean] whether a field is present
      def respond_to?(method)
        field = method.to_s
        field.chomp!('=')

        return true if @name_to_type.key?(field)
        super
      end

      # Returns value of the field.
      #
      # @param field [String, Integer] name or numeric index of the field
      # @return [Object] value of the field
      def [](field)
        case field
        when ::Integer
          return nil if field < 0 || field >= @fields.size
          @values[@fields[field][0]]
        when ::String
          @values[field]
        else
          raise ::ArgumentError, "unrecognized field #{field} in UDT: " \
            "#{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)}"
        end
      end

      # Returns value of the field.
      #
      # @param field [String, Integer] name or numeric index of the field to
      #   lookup
      #
      # @raise [IndexError] when numeric index given is out of bounds
      # @raise [KeyError] when field with a given name is not present
      # @raise [ArgumentError] when neither a numeric index nor a field name given
      #
      # @return [Object] value of the field
      def fetch(field)
        case field
        when ::Integer
          if field < 0 || field >= @fields.size
            raise ::IndexError,
                  "field index #{field} is not present in UDT: " \
                  "#{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)}"
          end
          @values[@fields[field][0]]
        when ::String
          unless @name_to_type.key?(field)
            raise ::KeyError,
                  "field #{field} is not defined in UDT: " \
                  "#{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)}"
          end
          @values[field]
        else
          raise ::ArgumentError, "unrecognized field #{field} in UDT: " \
            "#{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)}"
        end
      end

      # Sets value of the field.
      #
      # @param field [String, Integer] name or numeric index of the field
      # @param value [Object] new value for the field
      #
      # @raise [IndexError] when numeric index given is out of bounds
      # @raise [KeyError] when field with a given name is not present
      # @raise [ArgumentError] when neither a numeric index nor a field name
      #   given
      #
      # @return [Object] value.
      def []=(field, value)
        case field
        when ::Integer
          if field < 0 || field >= @fields.size
            raise ::IndexError,
                  "field index #{field} is not present in UDT: " \
                  "#{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)}"
          end
          Util.assert_type(@fields[field][1], value)
          @values[@fields[field][0]] = value
        when ::String
          unless @name_to_type.key?(field)
            raise ::KeyError,
                  "field #{field} is not defined in UDT: " \
                  "#{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)}"
          end
          Util.assert_type(@name_to_type[field], value)
          @values[field] = value
        else
          raise ::ArgumentError, "unrecognized field #{field} in UDT: " \
            "#{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)}"
        end
      end

      # Iterates over all fields of the UDT
      # @yieldparam name  [String] field name
      # @yieldparam value [Object] field value
      # @return [Cassandra::UDT] self
      def each(&block)
        @fields.each do |f|
          n = f.name
          yield(n, @values[n])
        end
        self
      end

      # Returns UDT size
      # @return [Integer] UDT size
      def size
        @fields.size
      end

      def inspect
        "#<Cassandra::UDT:0x#{object_id.to_s(16)} #{self}>"
      end

      def eql?(other)
        (other.is_a?(Strict) && @values.all? {|n, v| v == other[n]}) ||
          (other.is_a?(UDT) && other == self)
      end
      alias == eql?
    end

    include Enumerable

    # Creates a UDT instance
    # @param values [Hash<String, Object>, Array<Array<String, Object>>,
    #                 *Object, *Array<String, Object>] - UDT field values
    # @example Various ways of creating the same UDT instance
    #   Cassandra::UDT.new({'street' => '123 Main St.', 'city' => 'Whatever', 'state' => 'XZ', 'zip' => '10020'})
    #
    #   Cassandra::UDT.new(street: '123 Main St.', city: 'Whatever', state: 'XZ', zip: '10020')
    #
    #   Cassandra::UDT.new('street', '123 Main St.', 'city', 'Whatever', 'state', 'XZ', 'zip', '10020')
    #
    #   Cassandra::UDT.new(:street, '123 Main St.', :city, 'Whatever', :state, 'XZ', :zip, '10020')
    #
    #   Cassandra::UDT.new(['street', '123 Main St.'], ['city', 'Whatever'], ['state', 'XZ'], ['zip', '10020'])
    #
    #   Cassandra::UDT.new([:street, '123 Main St.'], [:city, 'Whatever'], [:state, 'XZ'], [:zip, '10020'])
    #
    #   Cassandra::UDT.new([['street', '123 Main St.'], ['city', 'Whatever'], ['state', 'XZ'], ['zip', '10020']])
    #
    #   Cassandra::UDT.new([[:street, '123 Main St.'], [:city, 'Whatever'], [:state, 'XZ'], [:zip, '10020']])
    def initialize(*values)
      values = Array(values.first) if values.one?

      Util.assert_not_empty(values,
                            'user-defined type must contain at least one value'
                           )

      if values.first.is_a?(::Array)
        @values = values.map do |pair|
          Util.assert(pair.size == 2,
                      'values of a user-defined type must be an Array of name and ' \
                      "value pairs, #{pair.inspect} given"
                     )
          name, value = pair

          [String(name), value]
        end
      else
        Util.assert(values.size.even?,
                    'values of a user-defined type must be an Array of alternating ' \
                    "names and values pairs, #{values.inspect} given"
                   )
        @values = values.each_slice(2).map do |(name, value)|
          [String(name), value]
        end
      end
    end

    # @!visibility public
    # Allows access to properties of a User-Defined Type.
    #
    # @example Getting and setting field values
    #   session.execute("CREATE TYPE address (street text, zipcode int)")
    #   session.execute("CREATE TABLE users (id int PRIMARY KEY, location frozen<address>)")
    #   row     = session.execute("SELECT * FROM users WHERE id = 123").first
    #   address = row['address']
    #
    #   puts address.street
    #   address.street = '123 SomePlace Cir'
    #
    # @overload method_missing(field)
    #   @param field [Symbol] name of the field to lookup
    #   @raise [NoMethodError] if the field is not present
    #   @return [Object] value of the field if present
    # @overload method_missing(field, value)
    #   @param field  [Symbol] name of the field (suffixed with `=`) to set
    #     the value for
    #   @param value [Symbol] new value for the field
    #   @raise [NoMethodError] if the field is not present
    #   @return [Cassandra::UDT] self.
    def method_missing(field, *args, &block)
      return super if block_given? || args.size > 1

      key = field.to_s
      set = !key.chomp!('=').nil?

      return super if set && args.empty?

      index = @values.index {|(name, _)| name == key}
      return super unless index

      if set
        @values[index][1] = args.first
      else
        @values[index][1]
      end
    end

    # Returns true if a field with a given name is present
    #
    # @param field [Symbol] method or name of the field
    #
    # @return [Boolean] whether a field is present
    def respond_to?(field)
      key = field.to_s
      key.chomp!('=')

      return true if @values.any? {|(name, _)| name == key}
      super
    end

    # Returns value of the field.
    #
    # @param field [String, Integer] name or numeric index of the field to
    #   lookup
    #
    # @raise [ArgumentError] when neither a numeric index nor a field name given
    #
    # @return [Object] value of the field, or nil if the field is not present
    def [](field)
      case field
      when ::Integer
        return nil if field >= 0 && field < @values.size

        @values[field][1]
      when ::String
        index = @values.index {|(n, _)| field == n}

        index && @values[index][1]
      else
        raise ::ArgumentError, "Unrecognized field #{field.inspect}"
      end
    end

    # Returns value of the field.
    #
    # @param field [String, Integer] name or numeric index of the field to
    #   lookup
    #
    # @raise [IndexError] when numeric index given is out of bounds
    # @raise [KeyError] when field with a given name is not present
    # @raise [ArgumentError] when neither a numeric index nor a field name given
    #
    # @return [Object] value of the field
    def fetch(field)
      case field
      when ::Integer
        if field >= 0 && field < @values.size
          raise ::IndexError, "Field index #{field.inspect} is not present"
        end

        @values[field][1]
      when ::String
        index = @values.index {|(n, _)| field == n}

        raise ::KeyError, "Unsupported field #{field.inspect}" unless index

        @values[index][1]
      else
        raise ::ArgumentError, "Unrecognized field #{field.inspect}"
      end
    end

    # @param field [String, Integer] name or numeric index of the field to
    #   lookup
    #
    # @return [Boolean] whether the field is present in this UDT
    def has_field?(field)
      case field
      when ::Integer
        return false if field < 0 || field >= @values.size
      when ::String
        return false unless @values.index {|(n, _)| field == n}
      else
        return false
      end

      true
    end

    alias include? has_field?

    # Sets value of the field.
    #
    # @param field [String] name of the field to set
    # @param value [Object] new value for the field
    #
    # @raise [IndexError] when numeric index given is out of bounds
    # @raise [KeyError] when field with a given name is not present
    # @raise [ArgumentError] when neither a numeric index nor a field name given
    #
    # @return [Object] value.
    def []=(field, value)
      case field
      when ::Integer
        if field < 0 || field >= @values.size
          raise ::IndexError, "Field index #{field.inspect} is not present"
        end

        @values[field][1] = value
      when ::String
        index = @values.index {|(n, _)| field == n}

        raise ::KeyError, "Unsupported field #{field.inspect}" unless index

        @values[index][1] = value
      else
        raise ::ArgumentError, "Unrecognized field #{field.inspect}"
      end
    end

    # Iterates over all fields of the UDT
    # @yieldparam name  [String] field name
    # @yieldparam value [Object] field value
    # @return [Cassandra::UDT] self
    def each(&block)
      @values.each {|(n, v)| yield(n, v)}
      self
    end

    # Returns UDT size
    # @return [Integer] UDT size
    def size
      @values.size
    end

    # Hash representation of the UDT
    def to_h
      @values.each_with_object(::Hash.new) do |(n, v), hash|
        hash[n] = v
      end
    end

    # String representation of the UDT
    def to_s
      '{ ' + @values.map {|(n, v)| "#{n}: #{v.inspect}"}.join(', ') + ' }'
    end

    # @private
    def inspect
      "#<Cassandra::UDT:0x#{object_id.to_s(16)} #{self}>"
    end

    # @private
    def eql?(other)
      other.is_a?(UDT) && @values.all? {|(n, v)| v == other[n]}
    end
    alias == eql?

    # @private
    def hash
      @values.inject(17) do |h, (n, v)|
        h = 31 * h + n.hash
        31 * h + v.hash
      end
    end
  end
end
