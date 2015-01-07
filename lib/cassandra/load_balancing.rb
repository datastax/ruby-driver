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
  module LoadBalancing
    # A list of possible load balancing distances that
    # {Cassandra::LoadBalancing::Policy#distance} must return
    DISTANCES = [:ignore, :local, :remote].freeze

    # @abstract Actual load balancing policies don't need to extend this class,
    #   only implement its methods. This class exists for documentation
    #   purposes only.
    class Policy
      # Allows policy to initialize with the cluster instance. This method is
      # called once before connecting to the cluster.
      #
      # @param cluster [Cassandra::Cluster] current cluster instance
      # @return [void]
      def setup(cluster)
      end

      # Allows policy to release any external resources it might be holding.
      # This method is called once the cluster has been terminated after calling
      # {Cassandra::Cluster#close}
      #
      # @param cluster [Cassandra::Cluster] current cluster instance
      # @return [void]
      def teardown(cluster)
      end

      # @see Cassandra::Listener#host_up
      def host_up(host)
      end

      # @see Cassandra::Listener#host_down
      def host_down(host)
      end

      # @see Cassandra::Listener#host_found
      def host_found(host)
      end

      # @see Cassandra::Listener#host_lost
      def host_lost(host)
      end

      # Returns a distance that determines how many connections (if any) the
      # driver will open to the host.
      #
      # @param host [Cassandra::Host] a host instance
      # @return [Symbol] distance to host. Must be one of
      #   {Cassandra::LoadBalancing::DISTANCES}
      def distance(host)
      end

      # Load balancing plan is used to determine the order in which hosts
      #   should be tried in case of a network failure.
      #
      # @note Hosts that should be ignored, must not be included in the Plan
      #
      # @param keyspace [String] current keyspace of the {Cassandra::Session}
      # @param statement [Cassandra::Statement] actual statement to be executed
      # @param options [Cassandra::Execution::Options] execution options to be used
      # @raise [NotImplementedError] override this method to return a plan
      # @return [Cassandra::LoadBalancing::Plan] a load balancing plan
      def plan(keyspace, statement, options)
      end

      # @return [String] a console-friendly representation of this policy
      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)}>"
      end
    end

    # A load balancing plan is used to determine the order of hosts for running
    # queries, preparing statements and establishing connections.
    # @abstract Plans returned by {Cassandra::LoadBalancing::Policy#plan}
    #   implementations don't need to extend this class, only implement its
    #   methods. This class exists for documentation purposes only.
    class Plan
      # @return [Boolean] whether the plan contains any more hosts
      def has_next?
      end

      # @return [Cql::Host] next host to try
      def next
      end
    end

    # @private
    class EmptyPlan
      def has_next?
        false
      end

      def next
        nil
      end
    end

    # @private
    EMPTY_PLAN = EmptyPlan.new
  end
end

require 'cassandra/load_balancing/policies'
