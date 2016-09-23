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

require File.dirname(__FILE__) + '/integration_test_case.rb'

class ExecutionProfilesTest < IntegrationTestCase
  def self.before_suite
    @@ccm_cluster = CCM.setup_cluster(1, 2)
  end

  def setup_schema
    @@ccm_cluster.setup_schema(<<-CQL)
    CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    USE simplex;
    CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT);
    CREATE TABLE test (k text, v int, PRIMARY KEY (k, v));
    CQL
  end

  def make_profile_without(attr)
    profile_hash = {
        consistency: :one,
        load_balancing_policy: Cassandra::LoadBalancing::Policies::RoundRobin.new,
        retry_policy: Cassandra::Retry::Policies::Default.new,
        timeout: nil
    }
    profile_hash.delete(attr)
    Cassandra::Execution::Profile.new(profile_hash)
  end

  # Test that the default execution profile requires load-balancing policy, retry-policy, and consistency to be set.
  #
  # test_default_execution_profile_validation tests that when the default execution profile is overridden,
  # load-balancing policy, retry-policy, and consistency are set. First, it creates a cluster with a legal
  # override of the default profile. Then it attempts to create cluster objects with each of the required
  # default-profile attributes missing and verifies that an ArgumentError occurs.
  #
  # @expected_errors [::ArgumentError] When a required attribute is missing in the default execution profile.
  #
  # @jira_ticket RUBY-256
  # @expected_result Successful cluster creation when all required profile attributes are set, errors otherwise.
  #
  # @test_assumptions A Cassandra cluster with at least one node.
  # @test_category argument:validation
  def test_default_execution_profile_validation
    profiles = {Cassandra::DEFAULT_EXECUTION_PROFILE => make_profile_without(nil)}
    cluster = Cassandra.cluster(execution_profiles: profiles)
    cluster.close

    # Now try without a consistency, load_balancing_policy, retry_policy.
    [:load_balancing_policy, :retry_policy, :consistency].each do |attr|
      profiles = { Cassandra::DEFAULT_EXECUTION_PROFILE => make_profile_without(attr) }
      assert_raises(ArgumentError, "removing #{attr} should raise") do
        Cassandra.cluster(execution_profiles: profiles)
      end
    end
  end
end
