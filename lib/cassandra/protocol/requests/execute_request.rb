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
    class ExecuteRequest < Request
      attr_reader :metadata, :values, :request_metadata, :serial_consistency, :page_size, :paging_state
      attr_accessor :consistency, :retries, :id

      def initialize(id, metadata, values, request_metadata, consistency, serial_consistency=nil, page_size=nil, paging_state=nil, trace=false)
        raise ArgumentError, "Metadata for #{metadata.size} columns, but #{values.size} values given" if metadata.size != values.size
        raise ArgumentError, %(No such consistency: #{consistency.inspect}) if consistency.nil? || !CONSISTENCIES.include?(consistency)
        raise ArgumentError, %(No such consistency: #{serial_consistency.inspect}) unless serial_consistency.nil? || CONSISTENCIES.include?(serial_consistency)
        raise ArgumentError, %(Paging state given but no page size) if paging_state && !page_size
        super(10, trace)
        @id = id
        @metadata = metadata
        @values = values
        @request_metadata = request_metadata
        @consistency = consistency
        @serial_consistency = serial_consistency
        @page_size = page_size
        @paging_state = paging_state
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
          buffer.append(flags.chr)
          if @values.size > 0
            encoder.write_parameters(buffer, @values, @metadata)
          end
          buffer.append_int(@page_size) if @page_size
          buffer.append_bytes(@paging_state) if @paging_state
          buffer.append_consistency(@serial_consistency) if @serial_consistency
        else
          encoder.write_parameters(buffer, @values, @metadata)
          buffer.append_consistency(@consistency)
        end
        buffer
      end

      def to_s
        id = @id.each_byte.map { |x| x.to_s(16) }.join('')
        %(EXECUTE #{id} #@values #{@consistency.to_s.upcase})
      end

      def eql?(rq)
        self.class === rq && rq.id == self.id && rq.metadata == self.metadata && rq.values == self.values && rq.consistency == self.consistency && rq.serial_consistency == self.serial_consistency && rq.page_size == self.page_size && rq.paging_state == self.paging_state
      end
      alias_method :==, :eql?

      def hash
        @h ||= begin
          h = 0
          h = ((h & 33554431) * 31) ^ @id.hash
          h = ((h & 33554431) * 31) ^ @metadata.hash
          h = ((h & 33554431) * 31) ^ @values.hash
          h = ((h & 33554431) * 31) ^ @consistency.hash
          h = ((h & 33554431) * 31) ^ @serial_consistency.hash
          h = ((h & 33554431) * 31) ^ @page_size.hash
          h = ((h & 33554431) * 31) ^ @paging_state.hash
          h
        end
      end
    end
  end
end
