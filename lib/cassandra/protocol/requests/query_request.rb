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
  module Protocol
    class QueryRequest < Request
      attr_reader :cql, :page_size, :paging_state, :payload, :serial_consistency,
                  :timestamp, :type_hints, :values
      attr_accessor :consistency, :retries

      def initialize(cql,
                     values,
                     type_hints,
                     consistency,
                     serial_consistency = nil,
                     page_size = nil,
                     paging_state = nil,
                     trace = false,
                     names = EMPTY_LIST,
                     timestamp = nil,
                     payload = nil)
        super(7, trace)
        @cql = cql
        @values = values
        @type_hints = type_hints
        @consistency = consistency
        @serial_consistency = serial_consistency
        @page_size = page_size
        @paging_state = paging_state
        @names = names
        @timestamp = timestamp
        @payload = payload
      end

      def payload?
        !!@payload
      end

      def write(buffer, protocol_version, encoder)
        buffer.append_long_string(@cql)
        buffer.append_consistency(@consistency)
        if protocol_version > 1
          flags  = 0
          flags |= 0x04 if @page_size
          flags |= 0x08 if @paging_state
          flags |= 0x10 if @serial_consistency
          flags |= 0x20 if protocol_version > 2 && @timestamp
          if @values && !@values.empty?
            flags |= 0x01
            flags |= 0x40 if protocol_version > 2 && !@names.empty?
            buffer.append(flags.chr)
            encoder.write_parameters(buffer, @values, @type_hints, @names)
          else
            if protocol_version < 5
              buffer.append(flags.chr)
            else
              buffer.append_int(flags)
            end
          end
          buffer.append_int(@page_size) if @page_size
          buffer.append_bytes(@paging_state) if @paging_state
          buffer.append_consistency(@serial_consistency) if @serial_consistency
          buffer.append_long(@timestamp) if protocol_version > 2 && @timestamp
        end
        buffer
      end

      def to_s
        %(QUERY "#{@cql}" #{@consistency.to_s.upcase})
      end

      def eql?(rq)
        rq.is_a?(self.class) &&
          rq.cql == cql &&
          rq.values == values &&
          rq.type_hints == type_hints &&
          rq.consistency == consistency &&
          rq.serial_consistency == serial_consistency &&
          rq.page_size == page_size &&
          rq.paging_state == paging_state
      end
      alias == eql?

      def hash
        @h ||= begin
          h = 17
          h = 31 * h + @cql.hash
          h = 31 * h + @values.hash
          h = 31 * h + @type_hints.hash
          h = 31 * h + @consistency.hash
          h = 31 * h + @serial_consistency.hash
          h = 31 * h + @page_size.hash
          h = 31 * h + @paging_state.hash
          h = 31 * h + @names.hash
          h = 31 * h + @timestamp.hash
          h
        end
      end
    end
  end
end
