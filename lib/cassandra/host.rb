# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
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
  class Host
    # @return [IPAddr] host ip that clients use to connect to this host.
    attr_reader :ip
    # @note Host id can be `nil` before cluster has connected.
    # @return [Cassandra::Uuid, nil] host id.
    attr_reader :id
    # @note Host datacenter can be `nil` before cluster has connected.
    # @return [String, nil] host datacenter
    attr_reader :datacenter
    # @note Host rack can be `nil` before cluster has connected.
    # @return [String, nil] host rack
    attr_reader :rack
    # @note Host's cassandra version can be `nil` before cluster has connected.
    # @return [String, nil] version of cassandra that a host is running
    attr_reader :release_version
    # @note Host tokens will be empty before cluster has connected.
    # @return [Array<String>] a list of tokens owned by this host
    attr_reader :tokens
    # @return [Symbol] host status. Must be `:up` or `:down`
    attr_reader :status
    # @note This is the public IP address of the host if the cluster is deployed across multiple Amazon EC2 regions
    #   (or equivalently multiple networks). Cassandra nodes in other EC2 regions use this address to connect to this
    #   host.
    # @return [IPAddr, String] broadcast address, if available.
    attr_reader :broadcast_address
    # @note This is the address that other Cassandra nodes use to connect to this host.
    # @return [IPAddr, String] listen address, if available.
    attr_reader :listen_address

    # @private
    def initialize(ip,
                   id = nil,
                   rack = nil,
                   datacenter = nil,
                   release_version = nil,
                   tokens = EMPTY_LIST,
                   status = :up,
                   broadcast_address = nil,
                   listen_address = nil)
      @ip              = ip
      @id              = id
      @rack            = rack
      @datacenter      = datacenter
      @release_version = release_version
      @tokens          = tokens
      @status          = status
      @broadcast_address = broadcast_address.is_a?(String) ?
          ::IPAddr.new(broadcast_address) : broadcast_address
      @listen_address = listen_address.is_a?(String) ?
          ::IPAddr.new(listen_address) : listen_address
    end

    # @return [Boolean] whether this host's status is `:up`
    def up?
      @status == :up
    end

    # @return [Boolean] whether this host's status is `:down`
    def down?
      @status == :down
    end

    # @private
    def hash
      @hash ||= begin
        h = 17
        h = 31 * h + @ip.hash
        h
      end
    end

    # @private
    def eql?(other)
      other.eql?(@ip)
    end
    alias == eql?

    # @private
    def inspect
      "#<#{self.class.name}:0x#{object_id.to_s(16)} @ip=#{@ip}>"
    end
  end
end
