# encoding: utf-8

require 'cql/load_balancing/distances'
require 'cql/load_balancing/policy'
require 'cql/load_balancing/policies'

module Cql
  module LoadBalancing
    DISTANCE_IGNORE = Distances::Ignore.new
    DISTANCE_LOCAL  = Distances::Local.new
    DISTANCE_REMOTE = Distances::Remote.new
  end
end
