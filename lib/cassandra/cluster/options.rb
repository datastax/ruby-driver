# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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
      attr_reader :auth_provider, :compressor, :connect_timeout, :credentials,
                  :heartbeat_interval, :idle_timeout, :port, :schema_refresh_delay,
                  :schema_refresh_timeout, :ssl

      attr_accessor :protocol_version

      def initialize(logger,
                     protocol_version,
                     credentials,
                     auth_provider,
                     compressor,
                     port,
                     connect_timeout,
                     ssl,
                     connections_per_local_node,
                     connections_per_remote_node,
                     heartbeat_interval,
                     idle_timeout,
                     synchronize_schema,
                     schema_refresh_delay,
                     schema_refresh_timeout,
                     client_timestamps,
                     nodelay,
                     requests_per_connection)
        @logger                 = logger
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
        @client_timestamps      = client_timestamps
        @nodelay                = nodelay

        @connections_per_local_node  = connections_per_local_node
        @connections_per_remote_node = connections_per_remote_node
        @requests_per_connection = requests_per_connection

        # If @protocol_version is nil, it means we want the driver to negotiate the
        # protocol starting with our known max (4). If @protocol_version is not nil,
        # it means the user wants us to use a particular version, so we should not
        # support negotiation.

        @protocol_negotiable = @protocol_version.nil?
        @protocol_version ||= 4
      end

      def protocol_negotiable?
        @protocol_negotiable
      end

      def synchronize_schema?
        @synchronize_schema
      end

      def client_timestamps?
        @client_timestamps
      end

      def nodelay?
        @nodelay
      end

      def compression
        @compressor && @compressor.algorithm
      end

      def create_authenticator(authentication_class)
        @auth_provider && @auth_provider.create_authenticator(authentication_class)
      end

      def connections_per_local_node
        # Return the option if set.
        return @connections_per_local_node if @connections_per_local_node

        # For v3 and later, default is 1 local connection.
        # For v2 and earlier, default is 2 local connections.
        # Return the default
        (@protocol_version > 2) ? 1 : 2
      end

      def connections_per_remote_node
        # Return the option if set; otherwise return the default (1).
        @connections_per_remote_node || 1
      end

      def requests_per_connection
        # There are a few possibilities here based on @requests_per_connection:
        # nil: default to 1024 for protocol 3 and later, 128 for < 3.
        # we're in v2 and value too high: return 128. We don't worry
        #   about this case for v3+ because option validation in
        #   Cassandra::cluster_async takes care of that.
        # good value: return it.
        #
        # NOTE: We can't compute and cache the result because protocol_version
        # can change over time in theory (if all nodes are upgraded to a new
        # version of Cassandra)

        # Return the default if option wasn't specified.
        default_requests_per_connection = @protocol_version > 2 ? 1024 : 128
        return default_requests_per_connection unless @requests_per_connection

        if @requests_per_connection > 128 && @protocol_version < 3
          @logger.warn(
            ":requests_per_connection setting of #{@requests_per_connection} is more " \
                'than the max of 128 for protocol v2. Falling back to 128.'
          )
          return 128
        end
        @requests_per_connection
      end
    end
  end
end
