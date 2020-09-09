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

require 'integration_test_case'

class DurationTest < IntegrationTestCase

  def setup
    @@ccm_cluster.setup_schema("CREATE KEYSPACE foo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
  end

  def test_can_insert_duration
    skip("Duration type was added in DSE 5.1/C* 3.10") if (CCM.dse_version < '5.1' || CCM.cassandra_version < '3.10')

    cluster = Cassandra.cluster
    session = cluster.connect "foo"

    durations = {
      'week' => Cassandra::Types.duration.new(0,7,0),
      'year' => Cassandra::Types.duration.new(12,0,0),
      'day' => Cassandra::Types.duration.new(0,1,0)
    }
    session.execute 'CREATE TABLE bar ("name" varchar, "dur" duration, primary key ("name"))'

    insert = Retry.with_attempts(5) { session.prepare "INSERT INTO foo.bar (name,dur) VALUES (?,?)" }
    durations.each_pair do |key,val|
      Retry.with_attempts(5) { session.execute insert, arguments: [key,val] }
    end

    result = session.execute("SELECT * FROM foo.bar")
    result.each do |row|
      duration = row["dur"]
      duration_name = row["name"]
      assert_equal durations[duration_name], duration
    end
  ensure
    cluster && cluster.close
  end
end
