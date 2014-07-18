# encoding: utf-8

module Cql
  module Reconnection
    module Policies
      class Constant
        class Schedule
          def initialize(interval)
            @interval = interval
          end

          def next
            @interval
          end
        end

        include Policy

        attr_reader :schedule

        def initialize(interval)
          @schedule = Schedule.new(interval)
        end
      end
    end
  end
end
