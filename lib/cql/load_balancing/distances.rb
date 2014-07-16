# encoding: utf-8

module Cql
  module LoadBalancing
    module Distances
      class Local
        def local?; true; end
        def remote?; false; end
        def ignore?; false; end
        def inspect; "#<#{self.class.name}>"; end
      end

      class Remote
        def local?; false; end
        def remote?; true; end
        def ignore?; false; end
        def inspect; "#<#{self.class.name}>"; end
      end

      class Ignore
        def local?; false; end
        def remote?; false; end
        def ignore?; true; end
        def inspect; "#<#{self.class.name}>"; end
      end
    end
  end
end
