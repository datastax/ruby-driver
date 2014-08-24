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
