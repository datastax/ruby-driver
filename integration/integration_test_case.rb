# encoding: utf-8

#--
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
#++

require File.dirname(__FILE__) + '/../support/ccm.rb'
require 'minitest/autorun'
require 'cassandra'

class IntegrationTestCase < MiniTest::Unit::TestCase
  def setup
    @ccm_cluster = CCM.setup_cluster(1, 1)

    $stop_cluster ||= begin
      at_exit do
        @ccm_cluster.stop
      end
    end
  end

  def teardown
    cluster = Cassandra.connect
    session = cluster.connect()

    cluster.each_keyspace do |keyspace|
      next if keyspace.name.start_with?('system')

      session.execute("DROP KEYSPACE #{keyspace.name}")
    end

    cluster.close
  end
end