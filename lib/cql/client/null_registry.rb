# encoding: utf-8

module Cql
  module Client
    # @private
    class NullRegistry
      attr_reader :ips, :listeners, :hosts

      def initialize(ips = [])
        @listeners = Set.new
        @ips       = ips
        @hosts     = Set.new(ips.map {|ip| Cql::Host.new(ip)})
      end

      def add_listener(listener)
        self
      end

      def remove_listener(listener)
        self
      end
    end
  end
end
