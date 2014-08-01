# encoding: utf-8

module Cql
  module Statements
    class Void
      include Statement

      def cql
        nil
      end
    end
  end
end
