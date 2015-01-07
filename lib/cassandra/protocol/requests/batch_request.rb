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
  module Protocol
    class BatchRequest < Request
      LOGGED_TYPE = 0
      UNLOGGED_TYPE = 1
      COUNTER_TYPE = 2

      attr_reader :type
      attr_accessor :consistency, :retries

      def initialize(type, consistency, trace=false)
        super(0x0D, trace)
        @type  = type
        @parts = []
        @consistency = consistency
      end

      def add_query(cql, values, types)
        @parts << [:simple, cql, values, types]
        nil
      end

      def add_prepared(id, values, types)
        @parts << [:prepared, id, values, types]
        nil
      end

      def write(buffer, protocol_version, encoder)
        buffer.append(@type.chr)
        buffer.append_short(@parts.size)

        @parts.each do |(statement_kind, *arguments)|
          __send__(:"write_#{statement_kind}", buffer, protocol_version, encoder, *arguments)
        end

        buffer.append_consistency(@consistency)

        if protocol_version > 2
          flags = 0

          buffer.append(flags.chr)
        end

        buffer
      end

      def to_s
        type_str = case @type
          when LOGGED_TYPE then 'LOGGED'
          when UNLOGGED_TYPE then 'UNLOGGED'
          when COUNTER_TYPE then 'COUNTER'
        end
        %(BATCH #{type_str} #{@parts.size} #{@consistency.to_s.upcase})
      end

      private

      QUERY_KIND = "\x00".freeze
      PREPARED_KIND = "\x01".freeze

      def write_simple(buffer, protocol_version, encoder, cql, values, types)
        buffer.append(QUERY_KIND)
        buffer.append_long_string(cql)
        encoder.write_parameters(buffer, values, types)
      end

      def write_prepared(buffer, protocol_version, encoder, id, values, types)
        buffer.append(PREPARED_KIND)
        buffer.append_short_bytes(id)
        encoder.write_parameters(buffer, values, types)
      end
    end
  end
end
