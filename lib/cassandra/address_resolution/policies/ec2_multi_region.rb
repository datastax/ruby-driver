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
  # Address Resolution policy allows translating a node ip address from what is
  # recorded in Cassandra's system tables to an actual ip address for the driver
  # to use. It is very useful in various multi-region scenarios (e.g. one EC2).
  module AddressResolution
    module Policies
      class EC2MultiRegion
        def initialize(resolver = Resolv)
          @resolver = resolver
        end

        # Returns original address.
        #
        # @param address [IPAddr] node ip address from Cassandra's system table
        #
        # @return [IPAddr] same as `address`
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
