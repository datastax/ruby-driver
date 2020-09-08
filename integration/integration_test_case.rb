# encoding: utf-8

#--
# Copyright DataStax, Inc.
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

require 'ccm.rb'
require 'retry.rb'
require 'schema_change_listener.rb'
require 'minitest/autorun'
require 'minitest/unit'
require 'cassandra'
require 'delorean'
require 'ansi/code'

class IntegrationTestCase < MiniTest::Unit::TestCase
  @@ccm_cluster = nil

  def self.before_suite
    @@ccm_cluster = CCM.setup_cluster(1, 1) unless self == IntegrationTestCase
  end

  def self.after_suite
  end

  def before_setup
    puts ANSI::Code.magenta("\n===== Begin #{self.__name__} ====")
  end

  def assert_columns(expected_names_and_types, actual_columns)
    assert_equal(expected_names_and_types.size, actual_columns.size)

    expected_names_and_types.zip(actual_columns) do |expected, actual_column|
      assert_equal expected[0], actual_column.name
      assert_equal expected[1], actual_column.type.kind
    end
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
