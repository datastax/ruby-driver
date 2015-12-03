# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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
      # @return [Array] positional arguments for the statement
      attr_reader :arguments
      # @return [Array] type hints for positional arguments for the statement
      attr_reader :type_hints

      # @return [String] paging state
      #
      # @note Although this feature exists to allow web applications to store
      #   paging state in an [HTTP cookie](http://en.wikipedia.org/wiki/HTTP_cookie), **it is not safe to
      #   expose without encrypting or otherwise securing it**. Paging state
      #   contains information internal to the Apache Cassandra cluster, such as
      #   partition key and data. Additionally, if a paging state is sent with CQL
      #   statement, different from the original, the behavior of Cassandra is
      #   undefined and will likely cause a server process of the coordinator of
      #   such request to abort.
      #
      # @see Cassandra::Result#paging_state
      attr_reader :paging_state

      # @return [nil, Hash<String, String>] custom outgoing payload, a map of
      # string and byte buffers.
      #
      # @see https://github.com/apache/cassandra/blob/33f1edcce97779c971d4f78712a9a8bf014ffbbc/doc/native_protocol_v4.spec#L127-L133 Description of custom payload in Cassandra native protocol v4.
      # @see https://datastax.github.io/java-driver/features/custom_payloads/#enabling-custom-payloads-on-c-nodes Enabling custom payloads on Cassandra nodes.
      #
      # @example Sending a custom payload
      #   result = session.execute(payload: {
      #              'some key' => Cassandra::Protocol::CqlByteBuffer.new
      #                                                              .append_string('some value')
      #            })
      attr_reader :payload

      # @private
      def initialize(options)
        consistency        = options[:consistency]
        page_size          = options[:page_size]
        trace              = options[:trace]
        timeout            = options[:timeout]
        serial_consistency = options[:serial_consistency]
        paging_state       = options[:paging_state]
        arguments          = options[:arguments]
        type_hints         = options[:type_hints]
        idempotent         = options[:idempotent]
        payload            = options[:payload]

        Util.assert_one_of(CONSISTENCIES, consistency) { ":consistency must be one of #{CONSISTENCIES.inspect}, #{consistency.inspect} given" }

        unless serial_consistency.nil?
          Util.assert_one_of(SERIAL_CONSISTENCIES, serial_consistency) { ":serial_consistency must be one of #{SERIAL_CONSISTENCIES.inspect}, #{serial_consistency.inspect} given" }
        end

        unless page_size.nil?
          page_size = Integer(page_size)
          Util.assert(page_size > 0) { ":page_size must be a positive integer, #{page_size.inspect} given" }
        end

        unless timeout.nil?
          Util.assert_instance_of(::Numeric, timeout) { ":timeout must be a number of seconds, #{timeout} given" }
          Util.assert(timeout > 0) { ":timeout must be greater than 0, #{timeout} given" }
        end

        unless paging_state.nil?
          paging_state = String(paging_state)
          Util.assert_not_empty(paging_state) { ":paging_state must not be empty" }
          Util.assert(!page_size.nil?) { ":page_size is required when :paging_state is given" }
        end

        if arguments.nil?
          arguments = EMPTY_LIST
        else
          Util.assert_instance_of_one_of([::Array, ::Hash], arguments) { ":arguments must be an Array or a Hash, #{arguments.inspect} given" }
        end

        if type_hints.nil?
          type_hints = EMPTY_LIST
        else
          Util.assert_instance_of_one_of([::Array, ::Hash], type_hints) { ":type_hints must be an Array or a Hash, #{type_hints.inspect} given" }
        end

        unless payload.nil?
          Util.assert_instance_of(::Hash, payload) { ":payload must be a Hash" }
          Util.assert_not_empty(payload) { ":payload must not be empty" }

          payload = payload.each_with_object(::Hash.new) do |(key, value), payload|
                      payload[String(key)] = String(value)
                    end
          payload.freeze
        end

        @consistency        = consistency
        @page_size          = page_size
        @trace              = !!trace
        @timeout            = timeout
        @serial_consistency = serial_consistency
        @paging_state       = paging_state
        @arguments          = arguments
        @type_hints         = type_hints
        @idempotent         = !!idempotent
        @payload            = payload
      end

      # @return [Boolean] whether request tracing was enabled
      def trace?
        @trace
      end

      # @return [Boolean] whether statement can be retried on timeout
      def idempotent?
        @idempotent
      end

      def eql?(other)
        other.is_a?(Options) &&
          other.consistency == @consistency &&
          other.page_size == @page_size &&
          other.trace? == @trace &&
          other.timeout == @timeout &&
          other.serial_consistency == @serial_consistency &&
          other.paging_state == @paging_state &&
          other.arguments == @arguments &&
          other.type_hints == @type_hints
      end
      alias :== :eql?

      # @private
      def override(*options)
        merged = options.unshift(to_h).inject do |base, opts|
          next base unless opts
          Util.assert_instance_of(::Hash, opts) { "options must be a Hash, #{options.inspect} given" }
          base.merge!(opts)
        end

        Options.new(merged)
      end

      # @private
      def to_h
        {
          :consistency        => @consistency,
          :page_size          => @page_size,
          :trace              => @trace,
          :timeout            => @timeout,
          :serial_consistency => @serial_consistency,
          :arguments          => @arguments || EMPTY_LIST,
          :type_hints         => @type_hints || EMPTY_LIST
        }
      end
    end
  end
end
