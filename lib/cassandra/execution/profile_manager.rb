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

      # Name of the default execution profile. Use this constant as the key for an execution profile when initializing
      # a {Cluster} to override the default execution profile with your own.
      DEFAULT_EXECUTION_PROFILE = '__DEFAULT_EXECUTION_PROFILE__'.freeze

      def initialize(default_profile, profiles)
        # Walk through the profiles and fill them out with attributes from the default profile when they're not
        # set. Also, save off all of the load-balancing policies for easy access.

        @load_balancing_policies = Set.new
        @load_balancing_policies << default_profile.load_balancing_policy if default_profile.load_balancing_policy
        @profiles = {DEFAULT_EXECUTION_PROFILE => default_profile}

        @unready_profiles = {}

        profiles.each do |name, profile|
          add_profile(name, profile)
        end
      end

      def default_profile
        @profiles[DEFAULT_EXECUTION_PROFILE]
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
        if !profile.well_formed? && @profiles.key?(profile.parent_name)
          # This profile is ready to inherit attributes from its parent.
          profile.merge_from(@profiles[profile.parent_name])
        end
        if profile.well_formed?
          make_available(name, profile)
          did_add = true
          while did_add && !@unready_profiles.empty?
            did_add = false
            @unready_profiles.dup.each do |name, profile|
              did_add = hydrate_profile(name, profile)
            end
          end
        else
          # This profile isn't ready to inherit its parent attributes yet
          @unready_profiles[name] = profile
        end
      end

      def make_available(name, profile)
        @profiles[name] = profile
        @load_balancing_policies << profile.load_balancing_policy if profile.load_balancing_policy
        @unready_profiles.delete(name)
      end

      def hydrate_profile(name, profile)
        did_work = false
        if @profiles.key?(profile.parent_name)
          profile.merge_from(@profiles[profile.parent_name])
          make_available(name, profile)
          did_work = true
        end
        did_work
      end

      # @private
      def inspect
        "#<#{self.class.name}:0x#{object_id.to_s(16)} " \
      "profiles=#{@profiles.inspect}>"
      end
    end
  end
end
