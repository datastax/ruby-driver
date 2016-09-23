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

      def initialize(profiles)
        @profiles = profiles
      end

      def default_profile
        @profiles[Cassandra::DEFAULT_EXECUTION_PROFILE]
      end

      def setup(cluster)
        lbp_broadcast(:setup, cluster)
      end

      def teardown(cluster)
        lbp_broadcast(:teardown, cluster)
      end

      def distance(host)
        # Return the min distance to the host, as per each lbp.
        distances = Set.new
        @profiles.each_value do |profile|
          distances.add(profile.load_balancing_policy.distance(host)) if profile.load_balancing_policy
        end
        return :local if distances.include?(:local)
        return :remote if distances.include?(:remote)

        # Fall back to ignore the host.
        return :ignore
      end

      def host_up(host)
        lbp_broadcast(:host_up, host)
      end

      def host_down(host)
        lbp_broadcast(:host_down, host)
      end

      def host_found(host)
        lbp_broadcast(:host_found, host)
      end

      def host_lost(host)
        lbp_broadcast(:host_lost, host)
      end

      # @private
      def inspect
        "#<#{self.class.name}:0x#{object_id.to_s(16)} " \
      "profiles=#{@profiles.inspect}>"
      end

      private

      def lbp_broadcast(method, *args)
        @profiles.each_value do |profile|
          profile.load_balancing_policy.send(method, *args) if profile.load_balancing_policy
        end
      end
    end
  end
end
