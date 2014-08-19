# encoding: utf-8

module Cql
  module Retry
    module Policies
      class Fallthrough
        include Policy

        def read_timeout(statement, consistency, required, received, retrieved, retries)
          reraise
        end

        def write_timeout(statement, consistency, type, required, received, retries)
          reraise
        end

        def unavailable(statement, consistency, required, alive, retries)
          reraise
        end
      end
    end
  end
end
