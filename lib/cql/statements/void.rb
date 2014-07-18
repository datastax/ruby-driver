# encoding: utf-8

module Cql
  module Statements
    class Void
      @@params = [].freeze

      def cql
        nil
      end

      def params
        @@params
      end
    end
  end
end
