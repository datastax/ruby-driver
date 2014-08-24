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

module Cassandra
  module LoadBalancing
    module Policy
      # @abstract implementation should be provided by an actual policy
      # @see Cassandra::Listener#host_up
      def host_up(host)
      end

      # @abstract implementation should be provided by an actual policy
      # @see Cassandra::Listener#host_down
      def host_down(host)
      end

      # @abstract implementation should be provided by an actual policy
      # @see Cassandra::Listener#host_found
      def host_found(host)
      end

      # @abstract implementation should be provided by an actual policy
      # @see Cassandra::Listener#host_lost
      def host_lost(host)
      end

      # Returns a distance that lets the driver to determine host many
      #   connections (if any) to open to the host
      #
      # @abstract implementation should be provided by an actual policy
      # @note a distance is constructed using one of
      #   {Cassandra::LoadBalancing::Policy#local},
      #   {Cassandra::LoadBalancing::Policy#remote} or
      #   {Cassandra::LoadBalancing::Policy#ignore}
      # @see Cassandra::LoadBalancing::Policy#local
      # @see Cassandra::LoadBalancing::Policy#remote
      # @see Cassandra::LoadBalancing::Policy#ignore
      # @param host [Cassandra::Host] a host instance
      # @return [Cassandra::LoadBalancing::Distance] distance to host
      def distance(host)
        ignore
      end

      # Load balancing plan is used to determine the order in which hosts
      #   should be tried in case of a network failure.
      #
      # @note Hosts that should be ignored, must not be included in the Plan
      #
      # @abstract implementation should be provided by an actual policy
      # @param keyspace [String] current keyspace of the {Cassandra::Session}
      # @param statement [Cassandra::Statement] actual statement to be executed
      # @param options [Cassandra::Execution::Options] execution options to be used
      # @raise [NotImplementedError] override this method to return a plan
      # @return [Cassandra::LoadBalancing::Plan] a load balancing plan
      def plan(keyspace, statement, options)
        raise ::NotImplementedError, "must be implemented by a child"
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)}>"
      end

      private

      # Returns a distance to host that is local
      # @return [Cassandra::LoadBalancing::Distance] tell driver to consider host
      #   local
      def local
        DISTANCE_LOCAL
      end

      # Returns a distance to host that is remote
      # @return [Cassandra::LoadBalancing::Distance] tell driver to consider host
      #   remote
      def remote
        DISTANCE_REMOTE
      end

      # Returns a distance to host that is ignored
      # @return [Cassandra::LoadBalancing::Distance] tell driver to ignore host
      def ignore
        DISTANCE_IGNORE
      end

      # @private
      class EmptyPlan
        def next
          raise ::StopIteration
        end
      end

      # @private
      EMPTY_PLAN = EmptyPlan.new
    end
  end
end
