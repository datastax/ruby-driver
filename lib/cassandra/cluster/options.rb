# encoding: utf-8

#--
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
#++

module Cassandra
  class Cluster
    # @private
    class Options
      attr_reader :credentials, :auth_provider, :compressor, :port,
                  :connect_timeout, :ssl, :heartbeat_interval, :idle_timeout,
                  :schema_refresh_delay, :schema_refresh_timeout
      attr_accessor :protocol_version

      def initialize(protocol_version, credentials, auth_provider, compressor, port, connect_timeout, ssl, connections_per_local_node, connections_per_remote_node, heartbeat_interval, idle_timeout, synchronize_schema, schema_refresh_delay, schema_refresh_timeout)
        @protocol_version       = protocol_version
        @credentials            = credentials
        @auth_provider          = auth_provider
        @compressor             = compressor
        @port                   = port
        @connect_timeout        = connect_timeout
        @ssl                    = ssl
        @heartbeat_interval     = heartbeat_interval
        @idle_timeout           = idle_timeout
        @synchronize_schema     = synchronize_schema
        @schema_refresh_delay   = schema_refresh_delay
        @schema_refresh_timeout = schema_refresh_timeout

        @connections_per_local_node  = connections_per_local_node
        @connections_per_remote_node = connections_per_remote_node
      end

      def synchronize_schema?
        @synchronize_schema
      end

      def compression
        @compressor && @compressor.algorithm
      end

      def create_authenticator(authentication_class)
        @auth_provider && @auth_provider.create_authenticator(authentication_class)
      end

      # increased number of streams in native protocol v3 allow for one
      # connections to be sufficient
      def connections_per_local_node
        (@protocol_version > 2) ? 1 : @connections_per_local_node
      end

      # increased number of streams in native protocol v3 allow for one
      # connections to be sufficient
      def connections_per_remote_node
        (@protocol_version > 2) ? 1 : @connections_per_remote_node
      end
    end
  end
end
