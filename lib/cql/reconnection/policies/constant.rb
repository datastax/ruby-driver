# encoding: utf-8

module Cql
  module Reconnection
    module Policies
      class Constant
        # @private
        class Schedule
          def initialize(interval)
            @interval = interval
          end

          def next
            @interval
          end
        end

        include Policy

        def initialize(interval)
          @schedule = Schedule.new(interval)
        end

        # @return [Cql::Reconnection::Schedule]
        def schedule
          @schedule
        end
      end
    end
  end
end
