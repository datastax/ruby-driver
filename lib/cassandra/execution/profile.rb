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
    # A profile is a collection of settings to use when executing and preparing statements. Register different
    # profiles when creating the {Cassandra::Cluster} and execute/prepare statements with a particular profile
    # by providing its name to the relevant method in {Session}.
    #
    # @see Cassandra.cluster
    # @see Session#execute_async
    # @see Session#execute
    # @see Session#prepare_async
    # @see Session#prepare
    class Profile
      # @return [Cassandra::LoadBalancing::Policy] load-balancing policy that determines which node will run the
      #   next statement.
      attr_reader :load_balancing_policy

      # @return [Cassandra::Retry::Policy] retry policy that determines how request retries should be handled for
      #   different failure modes.
      attr_reader :retry_policy

      # @return [Symbol] consistency level with which to run statements.
      attr_reader :consistency

      # @return [Numeric] request execution timeout in seconds. `nil` means there is no timeout.
      attr_reader :timeout

      # @private
      attr_accessor :parent_name

      # @private
      DEFAULT_OPTIONS = {load_balancing_policy: :unspecified,
                         retry_policy: :unspecified,
                         consistency: :unspecified,
                         timeout: :unspecified}.freeze

      # @private
      DEFAULT_PARENT_NAME = :default

      # @param options [Hash] hash of attributes. Unspecified attributes or attributes with nil values effectively
      #   fall back to the attributes in the default execution profile.
      # @option options [Numeric] :timeout (:unspecified) Request execution timeout in
      #   seconds. Setting value to `nil` will remove request timeout.
      # @option options [Cassandra::LoadBalancing::Policy] :load_balancing_policy (:unspecified) Load-balancing policy
      #   that determines which node will run the next statement.
      # @option options [Cassandra::Retry::Policy] :retry_policy (:unspecified) Retry policy that determines how
      #   request retries should be handled for different failure modes.
      # @option options [Symbol] :consistency (:unspecified) Consistency level with which to run statements. Must be one
      #   of {Cassandra::CONSISTENCIES}.
      def initialize(options = {})
        validate(options)
        options = DEFAULT_OPTIONS.merge(options)
        @load_balancing_policy = options[:load_balancing_policy]
        @retry_policy = options[:retry_policy]
        @consistency = options[:consistency]
        @timeout = options[:timeout]
        @parent_name = DEFAULT_PARENT_NAME
      end

      # @private
      def to_h
        {
          load_balancing_policy: @load_balancing_policy,
          retry_policy: @retry_policy,
          consistency: @consistency,
          timeout: @timeout
        }
      end

      # @private
      def eql?(other)
        other.is_a?(Profile) && \
          @load_balancing_policy == other.load_balancing_policy && \
          @retry_policy == other.retry_policy && \
          @consistency == other.consistency && \
          @timeout == other.timeout
      end
      alias == eql?

      # @private
      def hash
        @hash ||= begin
          h = 17
          h = 31 * h + @load_balancing_policy.hash
          h = 31 * h + @retry_policy.hash
          h = 31 * h + @consistency.hash
          h = 31 * h + @timeout.hash
          h
        end
      end

      # @private
      def inspect
        "#<#{self.class.name}:0x#{object_id.to_s(16)} " \
      "load_balancing_policy=#{@load_balancing_policy.inspect}, " \
      "retry_policy=#{@retry_policy.inspect}, " \
      "consistency=#{@consistency.inspect}, " \
      "timeout=#{@timeout.inspect}>"
      end

      # @private
      def validate(options)
        if options.key?(:timeout)
          timeout = options[:timeout]

          unless timeout.nil?
            Util.assert_instance_of(::Numeric, timeout, ":timeout must be a number of seconds, #{timeout.inspect} given")
            Util.assert(timeout > 0, ":timeout must be greater than 0, #{timeout} given")
          end
        end

        if options.key?(:load_balancing_policy)
          load_balancing_policy = options[:load_balancing_policy]
          methods = [:host_up, :host_down, :host_found, :host_lost, :setup, :teardown,
                     :distance, :plan]
          Util.assert_responds_to_all(methods, load_balancing_policy) do
            ":load_balancing_policy #{load_balancing_policy.inspect} must respond " \
            "to #{methods.inspect}, but doesn't"
          end
        end

        if options.key?(:retry_policy)
          retry_policy = options[:retry_policy]
          methods = [:read_timeout, :write_timeout, :unavailable]
          Util.assert_responds_to_all(methods, retry_policy) do
            ":retry_policy #{retry_policy.inspect} must respond to #{methods.inspect}, " \
            "but doesn't"
          end
        end

        if options.key?(:consistency)
          consistency = options[:consistency]
          Util.assert_one_of(CONSISTENCIES, consistency,
                             ":consistency must be one of #{CONSISTENCIES.inspect}, " \
                             "#{consistency.inspect} given")
        end
      end

      # @private
      def merge_from(parent_profile)
        return self if well_formed?

        parent_hash = parent_profile.to_h
        self_hash = to_h
        self_hash.each do |key, value|
          self_hash[key] = parent_hash[key] if value == :unspecified
        end
        Profile.new(self_hash)
      end

      # @private
      def well_formed?
        !@load_balancing_policy.nil? && @load_balancing_policy != :unspecified &&
            !@retry_policy.nil? && @retry_policy != :unspecified &&
            !@consistency.nil? && @consistency != :unspecified &&
            @timeout != :unspecified
      end
    end
  end
end
