# encoding: utf-8

module Cql
  module Retry
    module Policies
      class DowngradingConsistency
        include Policy

        def read_timeout(statement, consistency, required, received, retrieved, retries)
          return reraise if retries > 0 || SERIAL_CONSISTENCIES.include?(consistency)
          return max_likely_to_work(consistency, required, received) if received < required

          retrieved ? reraise : try_again(consistency)
        end

        def write_timeout(statement, consistency, type, required, received, retries)
          return reraise if retries > 0

          case type
          when :simple, :batch
            ignore
          when :unlogged_batch
            max_likely_to_work(consistency, required, received)
          when :batch_log
            try_again(consistency)
          else
            reraise
          end
        end

        def unavailable(statement, consistency, required, alive, retries)
          return reraise if retries > 0

          max_likely_to_work(consistency, required, alive)
        end

        private

        def max_likely_to_work(consistency, required, received)
          if consistency == :all && required > 1 && received >= (required.to_f / 2).floor + 1
            try_again(:quorum)
          elsif received >= 3
            try_again(:three)
          elsif received >= 2
            try_again(:two)
          elsif received >= 1
            try_again(:one)
          else
            reraise
          end
        end
      end
    end
  end
end
