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
  class Tuple
    # @private
    class Strict < Tuple
      attr_reader :types

      def initialize(types, values)
        @types  = types
        @values = values
      end

      def each(&block)
        @types.size.times do |i|
          yield(@values[i])
        end
        self
      end

      def [](i)
        @values[Integer(i)]
      end

      def fetch(i)
        i = Integer(i)
        raise ::IndexError, "index #{i} is outside of tuple, size: #{@types.size}" if i < 0 || i >= @types.size
        @values[i]
      end

      def []=(i, value)
        raise ::IndexError, "index #{i} is outside of tuple, size: #{@types.size}" if i < 0 || i >= @types.size
        Util.assert_type(@types[i], value)
        @values[i] = value
      end

      def size
        @types.size
      end

      def inspect
        "#<Cassandra::Tuple:0x#{object_id.to_s(16)} #{self}>"
      end
    end

    include Enumerable

    # Constructs a tuple with given values
    def initialize(*values)
      @values = values
    end

    # Iterates over all values of the tuple
    # @yieldparam value [Object] current value
    def each(&block)
      @values.each(&block)
      self
    end

    # @param i [Integer] numeric index of the value inside the tuple, must
    #   be `0 < i < tuple.size`
    # @return [Object] value of the tuple at position `i`
    def [](i)
      @values[Integer(i)]
    end

    # @param i [Integer] numeric index of the value inside the tuple, must
    #   be `0 < i < tuple.size`
    # @raise [IndexError] when index is outside of tuple bounds
    # @return [Object] value of the tuple at position `i`
    def fetch(i)
      i = Integer(i)
      raise ::IndexError, "index #{i} is outside of tuple, size: #{@values.size}" if i < 0 || i >= @values.size
      @values[i]
    end

    # @param i [Integer] numeric index of the value inside the tuple, must
    #   be `0 < i < tuple.size`
    # @param value [Object] a value to assign at position `i`
    # @raise [IndexError] when index is outside of tuple bounds
    # @return [Object] value of the tuple at position `i`
    def []=(i, value)
      i = Integer(i)
      raise ::IndexError, "index #{i} is outside of tuple, size: #{@values.size}" if i < 0 || i >= @values.size
      @values[i] = value
    end

    # Returns tuple size
    # @return [Integer] tuple size
    def size
      @values.size
    end

    # String representation of the tuple
    def to_s
      "(#{@values.map(&:to_s).join(', ')})"
    end

    # @private
    def inspect
      "#<Cassandra::Tuple:0x#{object_id.to_s(16)} #{self}>"
    end

    # @private
    def eql?(other)
      other == @values
    end
    alias == eql?

    # @private
    def hash
      @values.inject(17) {|h, v| 31 * h + v.hash}
    end
  end
end
