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
    class QueryRequest < Request
      attr_reader :cql, :values, :type_hints, :serial_consistency, :page_size, :paging_state
      attr_accessor :consistency, :retries

      def initialize(cql, values, type_hints, consistency, serial_consistency = nil, page_size = nil, paging_state = nil, trace = false)
        super(7, trace)
        @cql = cql
        @values = values
        @type_hints = type_hints
        @consistency = consistency
        @serial_consistency = serial_consistency
        @page_size = page_size
        @paging_state = paging_state
      end

      def write(buffer, protocol_version, encoder)
        buffer.append_long_string(@cql)
        buffer.append_consistency(@consistency)
        if protocol_version > 1
          flags  = 0
          flags |= 0x04 if @page_size
          flags |= 0x08 if @paging_state
          flags |= 0x10 if @serial_consistency
          if @values && @values.size > 0
            flags |= 0x01
            buffer.append(flags.chr)
            encoder.write_parameters(buffer, @values, @type_hints)
          else
            buffer.append(flags.chr)
          end
          buffer.append_int(@page_size) if @page_size
          buffer.append_bytes(@paging_state) if @paging_state
          buffer.append_consistency(@serial_consistency) if @serial_consistency
        end
        buffer
      end

      def to_s
        %(QUERY "#@cql" #{@consistency.to_s.upcase})
      end

      def eql?(rq)
        self.class === rq &&
          rq.cql == self.cql &&
          rq.values == self.values &&
          rq.type_hints == self.type_hints &&
          rq.consistency == self.consistency &&
          rq.serial_consistency == self.serial_consistency &&
          rq.page_size == self.page_size &&
          rq.paging_state == self.paging_state
      end
      alias_method :==, :eql?

      def hash
        h = 0xcbf29ce484222325
        h = 0xffffffffffffffff & (0x100000001b3 * (h ^ @cql.hash))
        h = 0xffffffffffffffff & (0x100000001b3 * (h ^ @values.hash))
        h = 0xffffffffffffffff & (0x100000001b3 * (h ^ @type_hints.hash))
        h = 0xffffffffffffffff & (0x100000001b3 * (h ^ @consistency.hash))
        h = 0xffffffffffffffff & (0x100000001b3 * (h ^ @serial_consistency.hash))
        h = 0xffffffffffffffff & (0x100000001b3 * (h ^ @page_size.hash))
        h = 0xffffffffffffffff & (0x100000001b3 * (h ^ @paging_state.hash))
        h
      end
    end
  end
end
