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
      #   paging state in an [HTTP cookie](http://en.wikipedia.org/wiki/HTTP_cookie),
      #   **it is not safe to expose without encrypting or otherwise securing it**.
      #   Paging state contains information internal to the Apache Cassandra cluster,
      #   such as partition key and data. Additionally, if a paging state is sent with
      #   CQL statement, different from the original, the behavior of Cassandra is
      #   undefined and will likely cause a server process of the coordinator of
      #   such request to abort.
      #
      # @see Cassandra::Result#paging_state
      attr_reader :paging_state

      # @return [nil, Hash<String, String>] custom outgoing payload, a map of
      # string and byte buffers.
      #
      # @see https://github.com/apache/cassandra/blob/cassandra-3.4/doc/native_protocol_v4.spec#L125-L131 Description
      #   of custom payload in Cassandra native protocol v4.
      # @see https://datastax.github.io/java-driver/manual/custom_payloads/#enabling-custom-payloads-on-c-nodes
      #   Enabling custom payloads on Cassandra nodes.
      #
      # @example Sending a custom payload
      #   result = session.execute(payload: {
      #              'some key' => Cassandra::Protocol::CqlByteBuffer.new
      #                                                              .append_string('some value')
      #            })
      attr_reader :payload

      # @private
      # @param options [Hash] execution options to validate and encapsulate
      # @param trusted_options [Options] (optional) base Execution::Options from which
      #        to create this new Options object.
      def initialize(options, trusted_options = nil)
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

        # consistency is a required attribute of an Options object. If we are creating
        # an Options object from scratch (e.g. no trusted_options as base) validate the
        # given consistency value (even if nil). Otherwise we're overlaying and only
        # validate the consistency option if given.
        if trusted_options.nil? || !consistency.nil?
          Util.assert_one_of(CONSISTENCIES, consistency) do
            ":consistency must be one of #{CONSISTENCIES.inspect}, " \
                "#{consistency.inspect} given"
          end
        end

        unless serial_consistency.nil?
          Util.assert_one_of(SERIAL_CONSISTENCIES, serial_consistency) do
            ":serial_consistency must be one of #{SERIAL_CONSISTENCIES.inspect}, " \
                "#{serial_consistency.inspect} given"
          end
        end

        unless page_size.nil?
          page_size = Integer(page_size)
          Util.assert(page_size > 0) do
            ":page_size must be a positive integer, #{page_size.inspect} given"
          end
        end

        unless timeout.nil?
          Util.assert_instance_of(::Numeric, timeout) do
            ":timeout must be a number of seconds, #{timeout} given"
          end
          Util.assert(timeout > 0) { ":timeout must be greater than 0, #{timeout} given" }
        end

        unless paging_state.nil?
          paging_state = String(paging_state)
          Util.assert_not_empty(paging_state) { ':paging_state must not be empty' }

          # We require page_size in either the new options or trusted options.
          Util.assert(!page_size.nil? ||
                          !(trusted_options.nil? || trusted_options.page_size.nil?)) do
            ':page_size is required when :paging_state is given'
          end
        end

        # :arguments defaults to empty-list, but we want to delegate to trusted_options
        # if it's set. So the logic is as follows:
        # If an arguments option was given, validate and use it regardless of anything
        # else.
        # Otherwise, if we have trusted_options, leave arguments nil for now so as not
        # to override trusted_options. Finally, if we don't have an arguments option
        # nor do we have trusted_options, fall back to the default empty-list.
        #
        # :type_hints works exactly the same way.
        if !arguments.nil?
          Util.assert_instance_of_one_of([::Array, ::Hash], arguments) do
            ":arguments must be an Array or a Hash, #{arguments.inspect} given"
          end
        elsif trusted_options.nil?
          arguments = EMPTY_LIST
        end

        if !type_hints.nil?
          Util.assert_instance_of_one_of([::Array, ::Hash], type_hints) do
            ":type_hints must be an Array or a Hash, #{type_hints.inspect} given"
          end
        elsif trusted_options.nil?
          type_hints = EMPTY_LIST
        end

        unless payload.nil?
          Util.assert_instance_of(::Hash, payload) { ':payload must be a Hash' }
          Util.assert_not_empty(payload) { ':payload must not be empty' }
          Util.assert(payload.size <= 65535) do
            ':payload cannot contain more than 65535 key/value pairs'
          end

          payload = payload.each_with_object(::Hash.new) do |(key, value), p|
            p[String(key)] = String(value)
          end
          payload.freeze
        end

        # Ok, validation is done. Time to save off all our values in our instance vars,
        # merging in values from trusted_options if it's set. To keep things readable,
        # we just put this into two branches of an if-else.

        if trusted_options.nil?
          @consistency = consistency
          @page_size = page_size
          @trace = !!trace
          @timeout = timeout
          @serial_consistency = serial_consistency
          @arguments = arguments
          @type_hints = type_hints
        else
          @consistency = consistency || trusted_options.consistency
          @page_size = page_size || trusted_options.page_size
          @trace = trace.nil? ? trusted_options.trace? : !!trace
          @timeout = timeout || trusted_options.timeout
          @serial_consistency = serial_consistency || trusted_options.serial_consistency
          @arguments = arguments || trusted_options.arguments
          @type_hints = type_hints || trusted_options.type_hints
        end

        # The following fields are *not* inherited from trusted_options, so we always
        # rely on the options we were given.
        @paging_state = paging_state
        @idempotent = !!idempotent
        @payload = payload
      end

      # @return [Boolean] whether request tracing was enabled
      def trace?
        @trace
      end

      # @return [Boolean] whether statement can be retried on timeout
      def idempotent?
        @idempotent
      end

      # @private
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
      alias == eql?

      # @private
      def override(*options)
        merged = options.unshift({}).inject do |base, opts|
          next base unless opts
          Util.assert_instance_of(::Hash, opts) do
            "options must be a Hash, #{options.inspect} given"
          end
          base.merge!(opts)
        end

        Options.new(merged, self)
      end
    end
  end
end
