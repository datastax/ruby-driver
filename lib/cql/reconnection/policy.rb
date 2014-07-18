# encoding: utf-8

module Cql
  module Reconnection
    module Policy
      def schedule
        raise ::NotImplementedError, "must be implemented by a child"
      end
    end
  end
end
