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
  module Protocol
    class ExecuteRequest < Request
      attr_reader :metadata, :request_metadata, :page_size, :paging_state, :payload,
                  :serial_consistency, :timestamp, :values
      attr_accessor :consistency, :id, :retries

      def initialize(id,
                     metadata,
                     values,
                     request_metadata,
                     consistency,
                     serial_consistency = nil,
                     page_size = nil,
                     paging_state = nil,
                     trace = false,
                     timestamp = nil,
                     payload = nil)
        if metadata.size != values.size
          raise ArgumentError, "Metadata for #{metadata.size} columns, but " \
              "#{values.size} values given"
        end
        if consistency.nil? || !CONSISTENCIES.include?(consistency)
          raise ArgumentError, %(No such consistency: #{consistency.inspect})
        end
        unless serial_consistency.nil? || CONSISTENCIES.include?(serial_consistency)
          raise ArgumentError, %(No such consistency: #{serial_consistency.inspect})
        end
        if paging_state && !page_size
          raise ArgumentError, %(Paging state given but no page size)
        end
        super(10, trace)
        @id = id
        @metadata = metadata
        @values = values
        @request_metadata = request_metadata
        @consistency = consistency
        @serial_consistency = serial_consistency
        @page_size = page_size
        @paging_state = paging_state
        @timestamp = timestamp
        @payload = payload
      end

      def payload?
        !!@payload
      end

      def write(buffer, protocol_version, encoder)
        buffer.append_short_bytes(@id)
        if protocol_version > 1
          buffer.append_consistency(@consistency)
          flags  = 0
          flags |= 0x01 if @values.size > 0
          flags |= 0x02 unless @request_metadata
          flags |= 0x04 if @page_size
          flags |= 0x08 if @paging_state
          flags |= 0x10 if @serial_consistency
          flags |= 0x20 if protocol_version > 2 && @timestamp
          buffer.append(flags.chr)
          encoder.write_parameters(buffer, @values, @metadata) if @values.size > 0
          buffer.append_int(@page_size) if @page_size
          buffer.append_bytes(@paging_state) if @paging_state
          buffer.append_consistency(@serial_consistency) if @serial_consistency
          buffer.append_timestamp(@timestamp) if protocol_version > 2 && @timestamp
        else
          encoder.write_parameters(buffer, @values, @metadata)
          buffer.append_consistency(@consistency)
        end
        buffer
      end

      def to_s
        id = @id.each_byte.map { |x| x.to_s(16) }.join('')
        %(EXECUTE #{id} #{@values} #{@consistency.to_s.upcase})
      end

      def eql?(rq)
        rq.is_a?(self.class) &&
          rq.id == id &&
          rq.metadata == metadata &&
          rq.values == values &&
          rq.consistency == consistency &&
          rq.serial_consistency == serial_consistency &&
          rq.page_size == page_size &&
          rq.paging_state == paging_state
      end
      alias == eql?

      def hash
        @h ||= begin
          h = 17
          h = 31 * h + @id.hash
          h = 31 * h + @metadata.hash
          h = 31 * h + @values.hash
          h = 31 * h + @consistency.hash
          h = 31 * h + @serial_consistency.hash
          h = 31 * h + @page_size.hash
          h = 31 * h + @paging_state.hash
          h = 31 * h + @timestamp.hash
          h
        end
      end
    end
  end
end
