# encoding: utf-8

module Cql
  module Retry
    module Policies
      class Default
        include Policy

        def read_timeout(statement, consistency_level, required_responses,
                         received_responses, data_retrieved, retries)
          return reraise if retries > 0

          if received_responses >= required_responses && !data_retrieved
            try_again(consistency_level)
          else
            reraise
          end
        end

        def write_timeout(statement, consistency_level, write_type,
                          acks_required, acks_received, retries)
          return reraise if retries > 0

          write_type == 'BATCH_LOG' ? try_again(consistency_level) : reraise
        end

        def unavailable(statement, consistency_level, replicas_required,
                        replicas_alive, retries)
          reraise
        end
      end
    end
  end
end
