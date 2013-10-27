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
        trace = false
        case options_or_consistency
        when Symbol
          consistency = options_or_consistency
        when Hash
          consistency = options_or_consistency[:consistency] || consistency
          timeout = options_or_consistency[:timeout]
          trace = options_or_consistency[:trace]
        end
        return consistency, timeout, trace
      end
    end
  end
end