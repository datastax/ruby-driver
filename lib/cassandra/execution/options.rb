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
  module Execution
    class Options
      # @return [Symbol] consistency for request. Must be one of
      #   {Cassandra::CONSISTENCIES}
      attr_reader :consistency
      # @return [Symbol] consistency for request with conditional updates
      #   (lightweight - compare-and-set, CAS - transactions). Must be one of
      #   {Cassandra::SERIAL_CONSISTENCIES}
      attr_reader :serial_consistency
      # @return [Integer] requested page size
      attr_reader :page_size
      # @return [Numeric] request timeout interval
      attr_reader :timeout

      # @private
      def initialize(options)
        consistency        = options[:consistency]
        page_size          = options[:page_size]
        trace              = options[:trace]
        timeout            = options[:timeout]
        serial_consistency = options[:serial_consistency]

        raise ::ArgumentError, ":consistency must be one of #{CONSISTENCIES.inspect}, #{consistency.inspect} given" unless CONSISTENCIES.include?(consistency)
        raise ::ArgumentError, ":serial_consistency must be one of #{SERIAL_CONSISTENCIES.inspect}, #{serial_consistency.inspect} given" if serial_consistency && !SERIAL_CONSISTENCIES.include?(serial_consistency)

        page_size = page_size && Integer(page_size)
        timeout   = timeout   && Integer(timeout)

        raise ::ArgumentError, ":page_size must be greater than 0, #{page_size.inspect} given" if page_size && page_size <= 0
        raise ::ArgumentError, ":timeout must be greater than 0, #{timeout.inspect} given" if timeout && timeout <= 0

        @consistency        = consistency
        @page_size          = page_size
        @trace              = !!trace
        @timeout            = timeout
        @serial_consistency = serial_consistency
      end

      # @return [Boolean] whether request tracing was enabled
      def trace?
        @trace
      end

      # @private
      def override(options)
        Options.new(to_h.merge!(options))
      end

      # @private
      def to_h
        {
          :consistency        => @consistency,
          :page_size          => @page_size,
          :trace              => @trace,
          :timeout            => @timeout,
          :serial_consistency => @serial_consistency
        }
      end
    end
  end
end
