# encoding: utf-8

module Cql
  module Client
    # @private
    class ExecuteOptionsDecoder
      def initialize(default_consistency)
        @default_consistency = default_consistency
        @default_options = {:consistency => @default_consistency}.freeze
      end

      def decode_options(*args)
        if args.empty?
          @default_options
        elsif args.size == 1
          decode_one(args.first)
        else
          args.each_with_object({}) do |options_or_consistency, result|
            result.merge!(decode_one(options_or_consistency))
          end
        end
      end

      private

      def decode_one(options_or_consistency)
        return @default_options unless options_or_consistency
        case options_or_consistency
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