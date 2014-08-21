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

class FakeClusterRegistry
  attr_reader :ips, :listeners, :hosts

  def initialize(ips = [])
    @listeners = Set.new
    @ips       = ips
    @hosts     = Set.new(ips.map {|ip| Cql::Host.new(ip)})
  end

  def add_listener(listener)
    @listeners << listener
    self
  end

  def remove_listener(listener)
    @listeners.delete(listener)
    self
  end
end

class FakeLoadBalancingPolicy
  def initialize(fake_cluster_registry)
    @registry = fake_cluster_registry
  end

  def distance(host)
    @registry.hosts.include?(host) ? Cql::LoadBalancing::DISTANCE_LOCAL : Cql::LoadBalancing::DISTANCE_IGNORE
  end

  def plan(keyspace, statement, options)
    @registry.hosts.to_enum
  end
end
