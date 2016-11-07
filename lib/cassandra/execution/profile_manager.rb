# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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
  module Execution
    # @private
    class ProfileManager
      attr_reader :profiles
      attr_reader :load_balancing_policies

      def initialize(default_profile, profiles)
        # default_profile is the default profile that we constructed internally. However, the user can override it
        # with their own :default profile. If that happens, ignore the internally generated default profile.

        profiles[:default] = default_profile unless profiles.key?(:default)

        # Save off all of the load-balancing policies for easy access.
        @load_balancing_policies = Set.new
        profiles.each do |name, profile|
          @load_balancing_policies << profile.load_balancing_policy
        end
        @profiles = profiles
      end

      def default_profile
        @profiles[:default]
      end

      def distance(host)
        # Return the min distance to the host, as per each lbp.
        distances = Set.new
        @load_balancing_policies.each do |lbp|
          distances.add(lbp.distance(host))
        end
        return :local if distances.include?(:local)
        return :remote if distances.include?(:remote)

        # Fall back to ignore the host.
        :ignore
      end

      # NOTE: It's only safe to call add_profile when setting up the cluster object. In particular,
      # this is only ok before calling Driver#connect.
      def add_profile(name, profile)
        @profiles[name] = profile
        @load_balancing_policies << profile.load_balancing_policy
      end

      # @private
      def inspect
        "#<#{self.class.name}:0x#{object_id.to_s(16)} " \
          "profiles=#{@profiles.inspect}>"
      end
    end
  end
end
