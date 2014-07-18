# encoding: utf-8

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

  def plan(keyspace, statement)
    @registry.hosts.to_enum
  end
end
