# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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

require File.dirname(__FILE__) + '/../support/ccm.rb'
require File.dirname(__FILE__) + '/../support/retry.rb'
require 'minitest/unit'
require 'minitest/autorun'
require 'cassandra'
require 'delorean'

class IntegrationTestCase < MiniTest::Unit::TestCase
  @@ccm_cluster = nil

  def self.before_suite
    @@ccm_cluster = CCM.setup_cluster(1, 1)
  end

  def self.after_suite
  end

  # Wait and retry until the given block returns a truthy value or
  # the timeout expires.
  #
  # @param timeout - timeout in seconds, may be fractional.
  # @param block - block to execute repeatedly; careful about side effects.
  # @return the result of the block.
  def assert_wait_and_retry_until(timeout, msg = nil, &block)
    expiration = Time.now + timeout
    result = nil
    while Time.now < expiration
      result = block.call
      break if result
      sleep(0.25)
    end

    assert result, msg
    result
  end
end

class IntegrationUnit < MiniTest::Unit
  def before_suites
  end

  def after_suites
  end

  def _run_suites(suites, type)
    begin
      before_suites
      super(suites, type)
    ensure
      after_suites
    end
  end

  def _run_suite(suite, type)
    begin
      suite.before_suite if suite.respond_to?(:before_suite)
      super(suite, type)
    ensure
      suite.after_suite if suite.respond_to?(:after_suite)
    end
  end
end

MiniTest::Unit.runner = IntegrationUnit.new
