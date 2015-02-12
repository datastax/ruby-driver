# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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
  module AddressResolution
    module Policies
      # This policy resolves private ips of the hosts in the same datacenter and
      # public ips of hosts in other datacenters.
      #
      # @note Initializing this policy is not necessary, you should just pass
      #   `:ec_multi_region` to the `:address_resolution` option of
      #   {Cassandra.cluster}
      class EC2MultiRegion
        # @private
        def initialize(resolver = Resolv)
          @resolver = resolver
        end

        # Returns ip address after a double DNS lookup. First, it will get
        # hostname from a given ip, then resolve the resulting hostname. This
        # policy works because AWS public hostnames resolve to a private ip
        # address within the same datacenter.
        #
        # @param address [IPAddr] node ip address from Cassandra's system table
        #
        # @return [IPAddr] private ip withing the same datacenter, public ip
        #   otherwise. Returns original address if DNS lookups fail.
        def resolve(address)
          @resolver.each_name(Resolv::DNS::Name.create(address.reverse)) do |name|
            @resolver.each_address(name) do |addr|
              return ::IPAddr.new(addr)
            end
          end

          # default to original address if reverse DNS lookup failed
          address
        end
      end
    end
  end
end
