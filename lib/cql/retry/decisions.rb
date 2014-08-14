# encoding: utf-8

module Cql
  module Retry
    # @private
    module Decisions
      class Retry
        attr_reader :consistency

        def initialize(consistency)
          @consistency = consistency
        end
      end

      class Reraise
      end

      class Ignore
      end
    end
  end
end
