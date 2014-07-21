# encoding: utf-8

module Cql
  module LoadBalancing
    module Policy
      def host_up(host)
        raise ::NotImplementedError, "must be implemented by a child"
      end

      def host_down(host)
        raise ::NotImplementedError, "must be implemented by a child"
      end

      def host_found(host)
        raise ::NotImplementedError, "must be implemented by a child"
      end

      def host_lost(host)
        raise ::NotImplementedError, "must be implemented by a child"
      end

      def distance(host)
        raise ::NotImplementedError, "must be implemented by a child"
      end

      def plan(keyspace, statement, options)
        raise ::NotImplementedError, "must be implemented by a child"
      end

      private

      def local
        DISTANCE_LOCAL
      end

      def remote
        DISTANCE_REMOTE
      end

      def ignore
        DISTANCE_IGNORE
      end

      class EmptyPlan
        def next
          raise ::StopIteration
        end
      end

      EMPTY_PLAN = EmptyPlan.new
    end
  end
end
