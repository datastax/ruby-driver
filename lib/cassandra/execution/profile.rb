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
      DEFAULT_OPTIONS = {load_balancing_policy: nil,
                         retry_policy: nil,
                         consistency: nil,
                         timeout: :unspecified}.freeze

      # @private
      DEFAULT_TIMEOUT = 12

      # @param options [Hash] hash of attributes. Unspecified attributes or attributes with nil values effectively
      #   fall back to the attributes in the default execution profile.
      # @option options [Numeric] :timeout (12) Request execution timeout in
      #   seconds. Setting value to `nil` will remove request timeout.
      # @option options [Cassandra::LoadBalancing::Policy] :load_balancing_policy (nil) Load-balancing policy that
      #   determines which node will run the next statement.
      # @option options [Cassandra::Retry::Policy] :retry_policy (nil) Retry policy that determines how request
      #   retries should be handled for different failure modes.
      # @option options [Symbol] :consistency (nil) Consistency level with which to run statements. Must be one
      #   of {Cassandra::CONSISTENCIES}.
      def initialize(options = {})
        options = DEFAULT_OPTIONS.merge(options)
        @load_balancing_policy = options[:load_balancing_policy]
        @retry_policy = options[:retry_policy]
        @consistency = options[:consistency]
        @timeout = options[:timeout]
      end

      def timeout
        @timeout == :unspecified ? DEFAULT_TIMEOUT : @timeout
      end

      # @private
      def to_h
        {
          load_balancing_policy: @load_balancing_policy,
          retry_policy: @retry_policy,
          consistency: @consistency,
          timeout: timeout
        }
      end

      # @private
      def eql?(other)
        other.is_a?(Profile) && \
          @load_balancing_policy == other.load_balancing_policy && \
          @retry_policy == other.retry_policy && \
          @consistency == other.consistency && \
          @timeout == other.instance_variable_get(:@timeout)
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
      def merge_from(parent_profile)
        @load_balancing_policy = parent_profile.load_balancing_policy if @load_balancing_policy.nil?
        @retry_policy = parent_profile.retry_policy if @retry_policy.nil?
        @consistency = parent_profile.consistency if @consistency.nil?
        @timeout = parent_profile.timeout if @timeout == :unspecified
        self
      end
    end
  end
end
