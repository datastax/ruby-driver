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
  class Cluster
    # @private
    class Registry
      include MonitorMixin

      def initialize(logger)
        @logger    = logger
        @hosts     = ::Hash.new
        @listeners = ::Array.new

        mon_initialize
      end

      def add_listener(listener)
        synchronize do
          listeners = @listeners.dup
          listeners.push(listener)
          @listeners = listeners
        end

        self
      end

      def remove_listener(listener)
        synchronize do
          listeners = @listeners.dup
          listeners.delete(listener)
          @listeners = listeners
        end

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
      alias hosts each_host

      def host(address)
        @hosts[address.to_s]
      end

      def has_host?(address)
        @hosts.key?(address.to_s)
      end

      def host_found(address, data = {})
        ip   = address.to_s
        host = @hosts[ip]

        if host
          if host.id              == data['host_id']         &&
             host.release_version == data['release_version'] &&
             host.rack            == data['rack']            &&
             host.datacenter      == data['data_center']

            return self if host.up?

            host = toggle_up(host)
          else
            @logger.debug("Host #{host.ip} metadata has been updated, it will be " \
                'considered lost and found')

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

        return self unless @hosts.key?(ip)

        synchronize do
          hosts  = @hosts.dup
          host   = hosts.delete(ip)
          @hosts = hosts
        end

        notify_lost(host)

        host
      end

      private

      def create_host(ip, data)
        Host.new(ip,
                 data['host_id'],
                 data['rack'],
                 data['data_center'],
                 data['release_version'],
                 Array(data['tokens']).freeze,
                 :up)
      end

      def toggle_up(host)
        host = Host.new(host.ip,
                        host.id,
                        host.rack,
                        host.datacenter,
                        host.release_version,
                        host.tokens,
                        :up)
        @logger.debug("Host #{host.ip} is up")
        @listeners.each do |listener|
          begin
            listener.host_up(host)
          rescue
            nil
          end
        end
        host
      end

      def toggle_down(host)
        host = Host.new(host.ip,
                        host.id,
                        host.rack,
                        host.datacenter,
                        host.release_version,
                        host.tokens,
                        :down)
        @logger.debug("Host #{host.ip} is down")
        @listeners.reverse_each do |listener|
          begin
            listener.host_down(host)
          rescue
            nil
          end
        end
        host
      end

      def notify_lost(host)
        if host.up?
          @logger.debug("Host #{host.ip} is down and lost")
          host = Host.new(host.ip,
                          host.id,
                          host.rack,
                          host.datacenter,
                          host.release_version,
                          host.tokens,
                          :down)
          @listeners.reverse_each do |listener|
            begin
              listener.host_down(host)
            rescue
              nil
            end
            begin
              listener.host_lost(host)
            rescue
              nil
            end
          end
        else
          @logger.debug("Host #{host.ip} is lost")
          @listeners.reverse_each do |listener|
            begin
              listener.host_lost(host)
            rescue
              nil
            end
          end
        end
      end

      def notify_found(host)
        @logger.debug("Host #{host.ip} is found and up")
        @listeners.each do |listener|
          begin
            listener.host_found(host)
          rescue
            nil
          end
          begin
            listener.host_up(host)
          rescue
            nil
          end
        end
      end
    end
  end
end
