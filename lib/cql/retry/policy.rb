# encoding: utf-8

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

module Cql
  module Retry
    module Policy
      # Decides wether to retry a read and at what consistency level.
      # 
      # @note this method may be called even if required_responses >= received
      #   responses if data_present is false.
      # 
      # @param statement [Cql::Statement] the original statement that timed out
      # @param consistency [Symbol] the original consistency level for the
      #   request, one of {Cql::CONSISTENCIES}
      # @param required [Integer] the number of responses required to achieve
      #   requested consistency level
      # @param received [Integer] the number of responses received by the time
      #   the query timed out
      # @param retrieved [Boolean] whether actual data (as opposed to data
      #   checksum) was present in the received responses.
      # @param retries [Integer] the number of retries already performed
      # 
      # @abstract implementation should be provided by an actual policy
      # @return [Cql::Policies::Retry::Decision] a retry decision
      #
      # @see Cql::Retry::Policy#try_again
      # @see Cql::Retry::Policy#reraise
      # @see Cql::Retry::Policy#ignore
      def read_timeout(statement, consistency, required, received, retrieved, retries)
      end

      # Decides wether to retry a write and at what consistency level.
      # 
      # @param statement [Cql::Statement] the original statement that timed out
      # @param consistency [Symbol] the original consistency level for the
      #   request, one of {Cql::CONSISTENCIES}
      # @param type [Symbol] One of `:simple`, `:batch`, `:unlogged_batch`,
      #   `:counter` or `:batch_log`
      # @param required [Integer] the number of acks required to achieve
      #   requested consistency level
      # @param received [Integer] the number of acks received by the time the
      #   query timed out
      # @param retries [Integer] the number of retries already performed
      # 
      # @abstract implementation should be provided by an actual policy
      # @return [Cql::Policies::Retry::Decision] a retry decision
      #
      # @see Cql::Retry::Policy#try_again
      # @see Cql::Retry::Policy#reraise
      # @see Cql::Retry::Policy#ignore
      def write_timeout(statement, consistency, type, required, received, retries)
      end

      # Decides wether to retry and at what consistency level on an Unavailable
      # exception.
      # 
      # @param statement [Cql::Statement] the original Statement that timed out
      # @param consistency [Symbol] the original consistency level for the
      #   request, one of {Cql::CONSISTENCIES}
      # @param required [Integer] the number of replicas required to achieve
      #   requested consistency level
      # @param alive [Integer] the number of replicas received by the time the
      #   query timed out
      # @param retries [Integer] the number of retries already performed
      # 
      # @abstract implementation should be provided by an actual policy
      # @return [Cql::Policies::Retry::Decision] a retry decision
      #
      # @see Cql::Retry::Policy#try_again
      # @see Cql::Retry::Policy#reraise
      # @see Cql::Retry::Policy#ignore
      def unavailable(statement, consistency, required, alive, retries)
      end

      private

      # Returns a decision that signals retry at a given consistency
      #
      # @param consistency [Symbol] consistency level for the retry, one of
      #   {Cql::CONSISTENCIES}
      # @return [Cql::Policies::Retry::Decision] tell driver to retry
      def try_again(consistency)
        Decisions::Retry.new(consistency)
      end

      # Returns a decision that signals to driver to reraise original error to
      # the application
      #
      # @return [Cql::Policies::Retry::Decision] tell driver to reraise
      def reraise
        DECISION_RERAISE
      end

      # Returns a decision that signals to driver to ignore the error
      #
      # @return [Cql::Policies::Retry::Decision] tell driver to ignore the error
      #   and return an empty result to the application
      def ignore
        DECISION_IGNORE
      end
    end
  end
end
