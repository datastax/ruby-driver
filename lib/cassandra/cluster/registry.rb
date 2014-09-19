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
    class Registry
      include MonitorMixin

      LISTENER_METHODS = [:host_found, :host_lost, :host_up, :host_down].freeze

      def initialize(logger)
        @logger    = logger
        @hosts     = ::Hash.new
        @listeners = ::Set.new

        mon_initialize
      end

      def add_listener(listener)
        synchronize { @listeners = @listeners.dup.add(listener) }

        self
      end

      def remove_listener(listener)
        synchronize { @listeners = @listeners.dup.delete(listener) }

        self
      end

      def each_host(&block)
        if block_given?
          @hosts.each_value(&block)
          self
        else
          @hosts.values
        end
      end
      alias :hosts :each_host

      def host(address)
        @hosts[address.to_s]
      end

      def has_host?(address)
        @hosts.has_key?(address.to_s)
      end

      def host_found(address, data = {})
        ip   = address.to_s
        host = @hosts[ip]

        if host
          if host.id              == data['host_id']         &&
             host.release_version == data['release_version'] &&
             host.rack            == data['rack']            &&
             host.datacenter      == data['data_center']

            return self
          else
            notify_lost(host)

            host = create_host(address, data)

            notify_found(host)
          end
        else
          host = create_host(address, data)

          notify_found(host)
        end

        synchronize do
          hosts     = @hosts.dup
          hosts[ip] = host
          @hosts    = hosts
        end

        self
      end

      def host_down(address)
        ip   = address.to_s
        host = @hosts[ip]

        return self unless host && !host.down?

        host = toggle_down(host)

        synchronize do
          hosts     = @hosts.dup
          hosts[ip] = host
          @hosts    = hosts
        end

        self
      end

      def host_up(address)
        ip   = address.to_s
        host = @hosts[ip]

        return self unless host && !host.up?

        host = toggle_up(host)

        synchronize do
          hosts     = @hosts.dup
          hosts[ip] = host
          @hosts    = hosts
        end

        self
      end

      def host_lost(address)
        ip   = address.to_s
        host = nil

        return self unless @hosts.has_key?(ip)

        synchronize do
          hosts  = @hosts.dup
          host   = hosts.delete(ip)
          @hosts = hosts
        end

        notify_lost(host)

        self
      end

      private

      def create_host(ip, data)
        Host.new(ip, data['host_id'], data['rack'], data['data_center'], data['release_version'], :up)
      end

      def toggle_up(host)
        host = Host.new(host.ip, host.id, host.rack, host.datacenter, host.release_version, :up)
        @logger.debug("Host #{host.ip} is up")
        @listeners.each do |listener|
          listener.host_up(host) rescue nil
        end
        host
      end

      def toggle_down(host)
        host = Host.new(host.ip, host.id, host.rack, host.datacenter, host.release_version, :down)
        @logger.debug("Host #{host.ip} is down")
        @listeners.each do |listener|
          listener.host_down(host) rescue nil
        end
        host
      end

      def notify_lost(host)
        if host.up?
          @logger.debug("Host #{host.ip} is down and lost")
          host = Host.new(host.ip, host.id, host.rack, host.datacenter, host.release_version, :down)
          @listeners.each do |listener|
            listener.host_down(host) rescue nil
            listener.host_lost(host) rescue nil
          end
        else
          @logger.debug("Host #{host.ip} is lost")
          @listeners.each do |listener|
            listener.host_lost(host) rescue nil
          end
        end
      end

      def notify_found(host)
        @logger.debug("Host #{host.ip} is found and up")
        @listeners.each do |listener|
          listener.host_found(host) rescue nil
          listener.host_up(host) rescue nil
        end
      end
    end
  end
end
