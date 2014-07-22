# encoding: utf-8

module Cql
  module Retry
    module Policies
      class Fallthrough
        include Policy

        def read_timeout(statement, consistency_level, required_responses,
                         received_responses, data_retrieved, attempt)
          reraise
        end

        def write_timeout(statement, consistency_level, write_type,
                          acks_required, acks_received, attempt)
          reraise
        end

        def unavailable(statement, consistency_level, replicas_required,
                        replicas_alive, attempt)
          reraise
        end
      end
    end
  end
end