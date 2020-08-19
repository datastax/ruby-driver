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
  module Types

    class Duration < Type
      include CustomData

      @@four_byte_max = 2 ** 32
      @@eight_byte_max = 2 ** 64

      # @private
      attr_reader :months, :days, :nanos

      # @private
      def initialize(months, days, nanos)
        super(:duration)
        @months = months
        @days = days
        @nanos = nanos
      end

      def new(*values)
        Util.assert_size(3, values, "Duration type expects three values, #{values.size} were provided")
        values.each { |v| Util.assert_type(Int, v) }
        Util.assert (Util.encode_zigzag32(values[0]) < @@four_byte_max), "Months value must be a valid 32-bit integer"
        Util.assert (Util.encode_zigzag32(values[1]) < @@four_byte_max), "Days value must be a valid 32-bit integer"
        Util.assert (Util.encode_zigzag64(values[2]) < @@eight_byte_max), "Nanos value must be a valid 32-bit integer"
        all_positive = values.all? {|i| i >= 0 }
        all_negative = values.all? {|i| i <= 0 }
        Util.assert (all_positive or all_negative), "Values in a duration must be uniformly positive or negative"
        Duration.new *values
      end

      def assert(value, message = nil, &block)
        Util.assert_instance_of(Duration, value, message, &block)
      end

      def to_s
        "Duration: months => #{@months}, days => #{@days}, nanos => #{@nanos}"
      end

      def hash
        @hash ||= begin
          h = 17
          h = 31 * h + @months.hash
          h = 31 * h + @days.hash
          h = 31 * h + @nanos.hash
          h
        end
      end

      def eql?(other)
        other.is_a?(Duration) &&
          @months == other.months &&
          @days == other.days &&
          @nanos == other.nanos
      end

      alias == eql?

      def self.cql_type
        Type.new(@kind)        
      end

      # Requirements for CustomData module
      def self.deserialize(bytestr)
        buffer = Cassandra::Protocol::CqlByteBuffer.new.append(bytestr)
        Cassandra::Types::Duration.new(buffer.read_signed_vint,buffer.read_signed_vint,buffer.read_signed_vint)
      end

      def self.type
        Cassandra::Types::Custom.new('org.apache.cassandra.db.marshal.DurationType')
      end

      def serialize
        rv = Cassandra::Protocol::CqlByteBuffer.new
        rv.append_signed_vint32(@months)
        rv.append_signed_vint32(@days)
        rv.append_signed_vint64(@nanos)
        rv
      end
    end
  end
end
