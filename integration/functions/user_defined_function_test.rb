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

require File.dirname(__FILE__) + '/../integration_test_case.rb'

class UserDefinedFunctionTest < IntegrationTestCase
  def setup
    @@ccm_cluster.setup_schema("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
  end

  # Test raising error for nonexistent UDF
  #
  # test_raise_error_on_nonexisting_udf tests the driver properly routes the Cassandra error for a nonexistent UDF. It
  # performs a simple SELECT query using an invalid UDF and verifies that a Cassandra::Errors::InvalidError is thrown.
  #
  # @expected_errors [Cassandra::Errors::InvalidError] When an the a nonexistent UDF is used.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-108
  # @expected_result a Cassandra::Errors::InvalidError should be raised
  #
  # @test_category functions:udf
  #
  def test_raise_error_on_nonexisting_udf
    skip("UDFs are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b text)")

    assert_raises(Cassandra::Errors::InvalidError) do
      session.execute("SELECT nonexisting(b) FROM mytable")
    end
  ensure
    cluster && cluster.close
  end

  # Test for creating a basic UDF
  #
  # test_can_create_udfs tests that a UDF can be created and its metadata is populated appropriately. It first creates
  # a simple UDT and verifies (after a 2 second sleep) that the keyspace metadata has been updated with the UDF's
  # existence. It then verifies each of the UDF's metadata.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-108
  # @expected_result A UDF should be created and its metadata should be populated.
  #
  # @test_category functions:udf
  #
  def test_can_create_udfs
    skip("UDFs are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    cluster = Cassandra.cluster(schema_refresh_delay: 0.1, schema_refresh_timeout: 0.1)
    session = cluster.connect("simplex")

    assert_empty cluster.keyspace("simplex").functions

    session.execute("CREATE FUNCTION sum_int(key int, val int)
                    RETURNS NULL ON NULL INPUT
                    RETURNS int
                    LANGUAGE javascript AS 'key + val'"
    )

    sleep(2) #give time for driver to receive schema change event
    assert cluster.keyspace("simplex").has_function?("sum_int")
    function = cluster.keyspace("simplex").function("sum_int")

    assert_equal "sum_int", function.name
    assert_equal "javascript", function.language
    assert_equal Cassandra::Types.int, function.type
    refute function.called_on_null?
    assert function.has_argument?("key")
    assert function.has_argument?("val")
    function.each_argument { |arg| assert_equal Cassandra::Types.int, arg.type }
  ensure
    cluster && cluster.close
  end

  # Test for creating two UDFs with the same name, but different types
  #
  # test_can_create_udf_same_name_different_types tests that a UDF is identified by its signature, which is the
  # combination of its name and its argument types. It first creates a UDF and retrieves it, verifying that the UDF's
  # signature contains the correct argument types. It then creates another UDF with the same name, but different
  # arguments. When retrieving the function, the first one is retrieved so the argument types are of the first UDF.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-108
  # @expected_result Two UDFs should be created, with the same name but different types
  #
  # @test_category functions:udf
  #
  def test_can_create_udf_same_name_different_types
    skip("UDFs are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    cluster = Cassandra.cluster(schema_refresh_delay: 0.1, schema_refresh_timeout: 0.1)
    session = cluster.connect("simplex")

    assert_empty cluster.keyspace("simplex").functions

    session.execute("CREATE FUNCTION sum_int(key int, val int)
                    RETURNS NULL ON NULL INPUT
                    RETURNS int
                    LANGUAGE javascript AS 'key + val'"
    )

    sleep(2)
    assert cluster.keyspace("simplex").has_function?("sum_int")
    function = cluster.keyspace("simplex").function("sum_int")
    function.each_argument { |arg| assert_equal Cassandra::Types.int, arg.type }

    session.execute("CREATE FUNCTION sum_int(key smallint, val smallint)
                    RETURNS NULL ON NULL INPUT
                    RETURNS int
                    LANGUAGE javascript AS 'key + val'"
    )

    sleep(2)
    assert cluster.keyspace("simplex").has_function?("sum_int")
    function = cluster.keyspace("simplex").function("sum_int")
    # keyspace#function retrieves the first UDF, so the first one is retrieved here
    function.each_argument { |arg| assert_equal Cassandra::Types.int, arg.type }
  ensure
    cluster && cluster.close
  end

  # Test for creating a UDF with no arguments
  #
  # test_can_create_function_no_argument tests that a UDF can be created without any arguments. It creates a simple
  # UDF without any arguments and verifies there are no arguments in the metadata.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-108
  # @expected_result UDF metadata should know no arguments
  #
  # @test_category functions:udf
  #
  def test_can_create_function_no_argument
    skip("UDFs are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    cluster = Cassandra.cluster(schema_refresh_delay: 0.1, schema_refresh_timeout: 0.1)
    session = cluster.connect("simplex")

    session.execute("CREATE FUNCTION print_time()
                    RETURNS NULL ON NULL INPUT
                    RETURNS bigint
                    LANGUAGE java AS 'return System.currentTimeMillis() / 1000L;'"
    )

    sleep(2)
    assert cluster.keyspace("simplex").has_function?("print_time")
    function = cluster.keyspace("simplex").function("print_time")
    assert_empty function.each_argument
  ensure
    cluster && cluster.close
  end

  # Test for maintaining metadata during keyspace changes
  #
  # test_functions_follow_keyspace_alter tests that UDF metadata is not change when there are other changes to the
  # keyspace metadata. It first creates a simple UDF and saves the keyspace UDF metadata. It then alters the keyspace
  # with durable_writes=false. Finally it verifies that there is no delta between the two function metadata in the
  # keyspace.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-108
  # @expected_result UDF metadata should not be changed due to keyspace changes
  #
  # @test_category functions:udf
  #
  def test_functions_follow_keyspace_alter
    skip("UDFs are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    cluster = Cassandra.cluster(schema_refresh_delay: 0.1, schema_refresh_timeout: 0.1)
    session = cluster.connect("simplex")

    session.execute("CREATE FUNCTION sum_int(key int, val int)
                    RETURNS NULL ON NULL INPUT
                    RETURNS int
                    LANGUAGE javascript AS 'key + val'"
    )

    sleep(2)
    original_functions = cluster.keyspace("simplex").functions

    session.execute("ALTER KEYSPACE simplex WITH durable_writes = false")
    new_functions = cluster.keyspace("simplex").functions
    assert_equal original_functions, new_functions

    session.execute("ALTER KEYSPACE simplex WITH durable_writes = true")
  ensure
    cluster && cluster.close
  end

  # Test for UDF null inputs
  #
  # test_cql_for_called_on_null tests that UDF metadata correctly populates function null input values. It creates a
  # UDF that returns null on null input and verifies in the that the function is not called on null input. It also checks
  # the generated cql string from the metadata for the null settings. It then creates a second UDF that can be called
  # on null input and similarly verifies the metadata.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-108
  # @expected_result UDF metadata should contain proper null settings
  #
  # @test_category functions:udf
  #
  def test_cql_for_called_on_null
    skip("UDFs are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    cluster = Cassandra.cluster(schema_refresh_delay: 0.1, schema_refresh_timeout: 0.1)
    session = cluster.connect("simplex")

    session.execute("CREATE FUNCTION sum_int(key int, val int)
                    RETURNS NULL ON NULL INPUT
                    RETURNS int
                    LANGUAGE javascript AS 'key + val'"
    )

    sleep(2)
    function = cluster.keyspace("simplex").function("sum_int")
    refute function.called_on_null?
    assert_match /RETURNS NULL ON NULL INPUT/, cluster.keyspace("simplex").function("sum_int").to_cql

    session.execute("CREATE FUNCTION sum_smallint(key smallint, val smallint)
                    CALLED ON NULL INPUT
                    RETURNS int
                    LANGUAGE javascript AS 'key + val'"
    )

    sleep(2)
    function = cluster.keyspace("simplex").function("sum_smallint")
    assert function.called_on_null?
    assert_match /CALLED ON NULL INPUT/, cluster.keyspace("simplex").function("sum_smallint").to_cql
  ensure
    cluster && cluster.close
  end

  # Test for invalid UDFs
  #
  # test_raise_error_on_invalid_udf tests that the driver properly raises an error for invalid UDFs. It creates an
  # invalid UDF and verifies that a Cassandra::Errors::InvalidError is thrown.
  #
  # @expected_errors [Cassandra::Errors::InvalidError] When an the an invalid is created.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-108
  # @expected_result a Cassandra::Errors::InvalidError should be raised
  #
  # @test_category functions:udf
  #
  def test_raise_error_on_invalid_udf
    skip("UDFs are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    assert_raises(Cassandra::Errors::InvalidError) do
      session.execute("CREATE FUNCTION IF NOT EXISTS sum_int (key int, val int)
                      RETURNS NULL ON NULL INPUT
                      RETURNS int
                      LANGUAGE javascript AS 'key ++ val';"
      )
    end
  ensure
    cluster && cluster.close
  end

  # Test for serialization and deserialization of UDFs
  #
  # test_udf_serialization_deserialization tests that the driver properly serializes and deserializes UDFs. It creates
  # a UDF which calculates the volume of an item. It also creates a table and inserts some data into it. Finally, it
  # calls the UDF on the 'dimensions' column of the table row and verifies that the proper dimensions is calculated
  # and returned via the driver.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-108
  # @expected_result the UDF should be created, and should return the proper dimensions of the item
  #
  # @test_category functions:udf
  #
  def test_udf_serialization_deserialization
    skip("UDFs are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    # Create the UDF
    session.execute("CREATE FUNCTION volume(dimensions tuple<double, double, double>)
                    RETURNS NULL ON NULL INPUT
                    RETURNS double
                    LANGUAGE java
                    AS 'return dimensions.getDouble(0) * dimensions.getDouble(1) * dimensions.getDouble(2);'"
    )

    # Create the table
    session.execute("CREATE TABLE inventory (item_id uuid PRIMARY KEY, dimensions Tuple<double,double,double>)")

    # Insert data
    insert = session.prepare("INSERT INTO inventory (item_id, dimensions) VALUES (?, ?)")
    session.execute(insert, arguments: [Cassandra::Uuid.new('0979dea5-5a65-446d-bad6-27d04d5dd8a5'),
                                        Cassandra::Tuple.new(2.96, 0.450, 0.100)]
    )

    # Verify UDF
    results = session.execute("SELECT item_id, volume(dimensions) FROM inventory WHERE item_id=0979dea5-5a65-446d-bad6-27d04d5dd8a5")
    assert_equal 0.1332, results.first["simplex.volume(dimensions)"]
  ensure
    cluster && cluster.close
  end

end