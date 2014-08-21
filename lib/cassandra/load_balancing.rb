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

require 'cassandra/load_balancing/distances'
require 'cassandra/load_balancing/policy'
require 'cassandra/load_balancing/policies'

module Cassandra
  module LoadBalancing
    # @private
    DISTANCE_IGNORE = Distances::Ignore.new
    # @private
    DISTANCE_LOCAL  = Distances::Local.new
    # @private
    DISTANCE_REMOTE = Distances::Remote.new
  end
end
