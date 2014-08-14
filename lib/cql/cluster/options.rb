# encoding: utf-8

module Cql
  class Cluster
    # @private
    class Options
      attr_reader :credentials, :auth_provider, :compressor, :port,
                  :connection_timeout, :connections_per_local_node, :connections_per_remote_node
      attr_accessor :protocol_version

      def initialize(protocol_version, credentials, auth_provider, compressor, port, connection_timeout, connections_per_local_node, connections_per_remote_node)
        @protocol_version   = protocol_version
        @credentials        = credentials
        @auth_provider      = auth_provider
        @compressor         = compressor
        @port               = port
        @connection_timeout = connection_timeout

        @connections_per_local_node  = connections_per_local_node
        @connections_per_remote_node = connections_per_remote_node
      end
    end
  end
end
