# encoding: utf-8

module Cql
  module Reconnection
    module Policies
      class Exponential
        class Schedule
          def initialize(start, max, exponent)
            @interval = start
            @max      = max
            @exponent = exponent
          end

          def next
            @interval.tap { backoff if @interval < @max }
          end

          private

          def backoff
            new_interval = @interval * @exponent

            if new_interval >= @max
              @interval = @max
            else
              @interval = new_interval
            end
          end
        end

        include Policy

        def initialize(start, max, exponent = 2)
          @start    = start
          @max      = max
          @exponent = exponent
        end

        def schedule
          Schedule.new(@start, @max, @exponent)
        end
      end
    end
  end
end
