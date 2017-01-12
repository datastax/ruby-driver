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
  class Duration
    include Cassandra::CustomData

    attr_reader :months
    attr_reader :days
    attr_reader :nanos

    def initialize(months, days, nanos)
      @months = months
      @days = days
      @nanos = nanos
    end

    def to_s
      months = @months < 0 ? -@months : @months
      days = @days < 0 ? -@days : @days
      nanos = @nanos < 0 ? -@nanos : @nanos
      negative = @months < 0 || @days < 0 || @nanos < 0 ? "-" : ""

      "#{negative}#{months}mo#{days}d#{nanos}ns"
    end

    # @private
    def inspect
      "#<Cassandra::Duration:0x#{object_id.to_s(16)} " \
        "@months=#{@months.inspect}, " \
        "@days=#{@days.inspect}, " \
        "@nanos=#{@nanos.inspect}>"
    end

    # @private
    def eql?(other)
      other.is_a?(Duration) && \
          @months == other.months && \
          @days == other.days && \
          @nanos == other.nanos
    end
    alias == eql?

    # @private
    def hash
      @hash ||= begin
        h = 17
        h = 31 * h + @months.hash
        h = 31 * h + @days.hash
        h = 31 * h + @nanos.hash
        h
      end
    end

    # methods related to serializing/deserializing.

    # @private
    TYPE = Cassandra::Types::Custom.new('org.apache.cassandra.db.marshal.DurationType')

    # @return [Cassandra::Types::Custom] type of column that is processed by this domain object class.
    def self.type
      TYPE
    end

    # Deserialize the given data into an instance of this domain object class.
    # @param data [String] byte-array representation of a column value of this custom type.
    # @return [Duration]
    # @raise [Cassandra::Errors::DecodingError] upon failure.
    def self.deserialize(data)
      buffer = Cassandra::Protocol::CqlByteBuffer.new(data)

      # TODO: Implement for duration..

      # little_endian = buffer.read(1) != "\x00"
      #
      # # Depending on the endian-ness of the data, we want to read it differently. Wrap the buffer
      # # with an "endian-aware" reader that reads the desired way.
      # buffer = Dse::Util::EndianBuffer.new(buffer, little_endian)
      #
      # type = buffer.read_unsigned
      # raise Cassandra::Errors::DecodingError, "Point data-type value should be 1, but was #{type}" if type != 1
      # deserialize_raw(buffer)
    end

    # Serialize this domain object into a byte array to send to Cassandra.
    # @return [String] byte-array representation of this domain object.
    def serialize
      buffer = Cassandra::Protocol::CqlByteBuffer.new

      # TODO: Implement for duration..

      # # Serialize little-endian.
      #
      # buffer << "\x01"
      #
      # # This is a point.
      # buffer.append([1].pack(Cassandra::Protocol::Formats::INT_FORMAT_LE))
      #
      # # Write out x and y.
      # serialize_raw(buffer)
      #
      # buffer
    end
  end
end
