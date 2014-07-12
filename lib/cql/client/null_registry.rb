# encoding: utf-8

module Cql
  module Client
    # @private
    class NullRegistry
      def add_listener(client); self; end
      def remove_listener(client); self; end
    end
  end
end
