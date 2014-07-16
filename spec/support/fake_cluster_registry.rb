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

