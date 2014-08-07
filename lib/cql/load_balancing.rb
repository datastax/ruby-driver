# encoding: utf-8

require 'cql/load_balancing/distances'
require 'cql/load_balancing/policy'
require 'cql/load_balancing/policies'

module Cql
  module LoadBalancing
    # @private
    DISTANCE_IGNORE = Distances::Ignore.new
    # @private
    DISTANCE_LOCAL  = Distances::Local.new
    # @private
    DISTANCE_REMOTE = Distances::Remote.new
  end
end
