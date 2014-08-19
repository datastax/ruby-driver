# encoding: utf-8

module Cql
  module Reconnection
    # @!parse [ruby]
    #   class Schedule
    #     # @return [Numeric] the next reconnection interval in seconds
    #     def next
    #     end
    #   end

    module Policy
      # Returns a reconnection schedule
      #
      # @abstract implementation should be provided by an actual policy
      # @note reconnection schedule doesn't need to extend
      #   {Cql::Reconnection::Schedule}, only conform to its interface
      # @return [Cql::Reconnection::Schedule] reconnection schedule
      def schedule
      end
    end
  end
end
