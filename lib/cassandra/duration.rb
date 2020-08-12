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
        Util.assert(values.size == 3) do
          "Duration type expects three values, #{values.size} were provided"
        end
        values.each { |v| Util.assert_type(Int, v) }
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

      def self.type
        Type.new(@kind)        
      end
    end    
  end

  class DurationTypeHandler
    def deserialize(bytestr)
      buffer = Cassandra::Protocol::CqlByteBuffer.new.append(bytestr)
      Cassandra::Types::Duration.new(buffer.read_signed_vint,buffer.read_signed_vint,buffer.read_signed_vint)
    end
  end
end
