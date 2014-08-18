# encoding: utf-8

module Cql
  module LoadBalancing
    module Policy
      # This method is called whenever a host is considered to be up, whether
      #   by Cassandra's gossip exchange or when the driver has successfully
      #   established a connection to it.
      #
      # @param host [Cql::Host] a host instance
      # @abstract implementation should be provided by an actual policy
      # @return [void]
      def host_up(host)
      end

      # This method is called whenever a host is considered to be down, whether
      #   by Cassandra's gossip exchange or when the driver failed to establish
      #   any connections to it.
      #
      # @param host [Cql::Host] a host instance
      # @abstract implementation should be provided by an actual policy
      # @return [void]
      def host_down(host)
      end

      # This method is called whenever a host is discovered by the driver,
      #   whether because it is a completely new node or if its
      #   {Cql::Host#datacenter} or {Cql::Host#rack} have changed.
      #
      # @param host [Cql::Host] a host instance
      # @abstract implementation should be provided by an actual policy
      # @return [void]
      def host_found(host)
      end

      # This method is called whenever a host leaves the cluster, whether
      #   because it is completely gone or if its {Cql::Host#datacenter} or
      #   {Cql::Host#rack} have changed.
      #
      # @param host [Cql::Host] a host instance
      # @abstract implementation should be provided by an actual policy
      # @return [void]
      def host_lost(host)
      end

      # Returns a distance that lets the driver to determine host many
      #   connections (if any) to open to the host
      #
      # @param host [Cql::Host] a host instance
      # @abstract implementation should be provided by an actual policy
      # @return [Cql::LoadBalancing::Distance] distance to host
      # @note a distance is constructed using one of
      #   {Cql::LoadBalancing::Policy#local},
      #   {Cql::LoadBalancing::Policy#remote} or
      #   {Cql::LoadBalancing::Policy#ignore}
      # @see Cql::LoadBalancing::Policy#local
      # @see Cql::LoadBalancing::Policy#remote
      # @see Cql::LoadBalancing::Policy#ignore
      def distance(host)
        ignore
      end

      # Load balancing plan is used to determine the order in which hosts
      #   should be tried in case of a network failure.
      #
      # @note Hosts that should be ignored, must not be included in the Plan
      #
      # @param keyspace [String] current keyspace of the {Cql::Session}
      # @param statement [Cql::Statement] actual statement to be executed
      # @param options [Cql::Execution::Options] execution options to be used
      # @abstract implementation should be provided by an actual policy
      # @return [Cql::LoadBalancing::Plan] a load balancing plan
      # @raise [NotImplementedError] override this method to return a plan
      def plan(keyspace, statement, options)
        raise ::NotImplementedError, "must be implemented by a child"
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)}>"
      end

      private

      # Returns a distance to host that is local
      # @return [Cql::LoadBalancing::Distance] tell driver to consider host
      #   local
      def local
        DISTANCE_LOCAL
      end

      # Returns a distance to host that is remote
      # @return [Cql::LoadBalancing::Distance] tell driver to consider host
      #   remote
      def remote
        DISTANCE_REMOTE
      end

      # Returns a distance to host that is ignored
      # @return [Cql::LoadBalancing::Distance] tell driver to ignore host
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
