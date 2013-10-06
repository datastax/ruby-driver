# encoding: utf-8

module Cql
  module Client
    class ExecuteOptionsDecoder
      def initialize(default_consistency)
        @default_consistency = default_consistency
      end

      def decode_options(options_or_consistency)
        consistency = @default_consistency
        timeout = nil
        case options_or_consistency
        when Symbol
          consistency = options_or_consistency
        when Hash
          consistency = options_or_consistency[:consistency] || consistency
          timeout = options_or_consistency[:timeout]
        end
        return consistency, timeout
      end
    end
  end
end