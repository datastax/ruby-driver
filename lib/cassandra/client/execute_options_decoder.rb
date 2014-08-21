# encoding: utf-8

# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Cassandra
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