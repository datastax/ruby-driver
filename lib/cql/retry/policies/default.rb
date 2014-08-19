# encoding: utf-8

module Cql
  module Retry
    module Policies
      class Default
        include Policy

        def read_timeout(statement, consistency, required, received, retrieved, retries)
          return reraise if retries > 0

          if received >= required && !retrieved
            try_again(consistency)
          else
            reraise
          end
        end

        def write_timeout(statement, consistency, type, required, received, retries)
          return reraise if retries > 0

          type == :batch_log ? try_again(consistency) : reraise
        end

        def unavailable(statement, consistency, required, alive, retries)
          reraise
        end
      end
    end
  end
end
