# encoding: utf-8

module Cql
  module Retry
    module Policies
      class DowngradingConsistency
        include Policy

        def read_timeout(statement, consistency_level, required_responses,
                         received_responses, data_retrieved, retries)
          return reraise if retries > 0 || SERIAL_CONSISTENCIES.include?(consistency_level)
          return max_likely_to_work(consistency_level, required_responses, received_responses) if received_responses < required_responses

          data_retrieved ? reraise : try_again(consistency_level)
        end

        def write_timeout(statement, consistency_level, write_type,
                          acks_required, acks_received, retries)
          return reraise if retries > 0

          case write_type
          when 'SIMPLE', 'BATCH'
            ignore
          when 'UNLOGGED_BATCH'
            max_likely_to_work(consistency_level, acks_required, acks_received)
          when 'BATCH_LOG'
            try_again(consistency_level)
          else
            reraise
          end
        end

        def unavailable(statement, consistency_level, replicas_required,
                        replicas_alive, retries)
          return reraise if retries > 0

          max_likely_to_work(consistency_level, replicas_required, replicas_alive)
        end

        private

        def max_likely_to_work(consistency_level, required, received)
          if consistency_level == :all && required > 1 && received >= (required.to_f / 2).floor + 1
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
