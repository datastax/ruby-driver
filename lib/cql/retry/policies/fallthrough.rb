# encoding: utf-8

module Cql
  module Retry
    module Policies
      class Fallthrough
        include Policy

        def read_timeout(statement, consistency_level, required_responses,
                         received_responses, data_retrieved, attempts)
          reraise
        end

        def write_timeout(statement, consistency_level, write_type,
                          acks_required, acks_received, attempts)
          reraise
        end

        def unavailable(statement, consistency_level, replicas_required,
                        replicas_alive, attempts)
          reraise
        end
      end
    end
  end
end
