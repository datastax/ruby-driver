# encoding: utf-8

module Cql
  module Client
    # @private
    class ExecuteOptionsDecoder
      def initialize(default_consistency)
        @default_consistency = default_consistency
        @default_options = {:consistency => @default_consistency}.freeze
      end

      def decode_options(options_or_consistency)
        case options_or_consistency
        when nil
          @default_options
        when Symbol
          {:consistency => options_or_consistency}
        when Hash
          if options_or_consistency.include?(:consistency)
            options_or_consistency
          else
            options = options_or_consistency.dup
            options[:consistency] = @default_consistency
            options
          end
        end
      end
    end
  end
end