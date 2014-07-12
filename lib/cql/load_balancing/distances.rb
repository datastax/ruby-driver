# encoding: utf-8

module Cql
  module LoadBalancing
    module Distances
      class Local
        def pool_size
          2
        end
      end

      class Remote
        def pool_size
          1
        end
      end

      class Ignore
        def pool_size
          0
        end
      end
    end
  end
end
