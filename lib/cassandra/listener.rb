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
  # Cassandra state listener.
  #
  # @note Actual state listener implementations don't need to inherit from this
  #   class as long as they conform to its interface. This class exists solely
  #   for documentation purposes
  class Listener
    # This method is called whenever a host is considered to be up, whether
    #   by Cassandra's gossip exchange or when the driver has successfully
    #   established a connection to it.
    #
    # @param host [Cassandra::Host] a host instance
    # @return [void]
    def host_up(host)
    end

    # This method is called whenever a host is considered to be down, whether
    #   by Cassandra's gossip exchange or when the driver failed to establish
    #   any connections to it.
    #
    # @param host [Cassandra::Host] a host instance
    # @return [void]
    def host_down(host)
    end

    # This method is called whenever a host is discovered by the driver,
    #   whether because it is a completely new node or if its
    #   {Cassandra::Host#datacenter} or {Cassandra::Host#rack} have changed.
    #
    # @param host [Cassandra::Host] a host instance
    # @return [void]
    def host_found(host)
    end

    # This method is called whenever a host leaves the cluster, whether
    #   because it is completely gone or if its {Cassandra::Host#datacenter} or
    #   {Cassandra::Host#rack} have changed.
    #
    # @param host [Cassandra::Host] a host instance
    # @return [void]
    def host_lost(host)
    end

    # This method is called whenever a new keyspace is created.
    #
    # @param host [Cassandra::Keyspace] a keyspace instance
    # @return [void]
    def keyspace_created(keyspace)
    end

    # This method is called whenever an existing keyspace is changed. This
    # happens when a new table is created or an existing table is dropped or
    # altered.
    #
    # @param host [Cassandra::Keyspace] a keyspace instance
    # @return [void]
    def keyspace_changed(keyspace)
    end

    # This method is called whenever an existing keyspace is dropped.
    #
    # @param host [Cassandra::Keyspace] a keyspace instance
    # @return [void]
    def keyspace_dropped(keyspace)
    end
  end
end
