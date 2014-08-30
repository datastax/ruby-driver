Feature: Datatypes

  A columnfamily can have all the datatypes defined in Cassandra.
  [See here](http://www.datastax.com/documentation/cql/3.0/cql/cql_reference/cql_data_types_c.html)
  for a full list of datatypes and their differences.

  Background:
    Given a running cassandra cluster with a keyspace "simplex"

  Scenario: Text-like datatypes are inserted into a column family
    Given the following example:
    """ruby
      require 'cassandra'

      cluster = Cassandra.connect
      at_exit { cluster.close }

      session = cluster.connect("simplex")
      session.execute("CREATE TABLE mytable (
        a int PRIMARY KEY,
        b ascii,
        c blob,
        d text,
        e varchar,
      )", consistency: :all)
      sleep(1) # wait for the change to propagate

      insert = session.prepare("INSERT INTO mytable (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)")
      session.execute(insert, 0, 'ascii', "blob", 'text', 'varchar')

      row = session.execute("SELECT * FROM mytable").first

      puts "Ascii: #{row['b']}"
      puts "Blob: #{row['c']}"
      puts "Text: #{row['d']}"
      puts "Varchar: #{row['e']}"
      """
    When it is executed
    Then its output should contain:
      """
      Ascii: ascii
      Blob: blob
      Text: text
      Varchar: varchar
      """

  Scenario: Integer-like datatypes are inserted into a column family
    Given the following example:
    """ruby
      require 'cassandra'

      cluster = Cassandra.connect
      at_exit { cluster.close }

      session = cluster.connect("simplex")
      session.execute("CREATE TABLE mytable (
        a int PRIMARY KEY,
        b bigint,
        c decimal,
        d double,
        e float,
        f int,
        g varint
      )", consistency: :all)
      sleep(1) # wait for the change to propagate

      insert = session.prepare("INSERT INTO mytable (a, b, c, d, e, f, g) VALUES (?, ?, ?, ?, ?, ?, ?)")
      session.execute(insert, 0, 765438000, BigDecimal.new('1313123123.234234234234234234123'),
                                  Math::PI, 3.14, 4, 67890656781923123918798273492834712837198237)

      row = session.execute("SELECT * FROM mytable").first

      puts "Bigint: #{row['b']}"
      puts "Decimal: #{row['c']}"
      puts "Double: #{row['d']}"
      puts "Float: #{row['e']}"
      puts "Integer: #{row['f']}"
      puts "Varint: #{row['g']}"
      """
    When it is executed
    Then its output should contain:
      """
      Bigint: 765438000
      Decimal: 0.1313123123234234234234234234123E10
      Double: 3.141592653589793
      Float: 3.140000104904175
      Integer: 4
      Varint: 67890656781923123918798273492834712837198237
      """

  Scenario: ID-like datatypes are inserted into a column family
    Given the following example:
    """ruby
      require 'cassandra'
      require 'time'

      cluster = Cassandra.connect
      at_exit { cluster.close }

      session = cluster.connect("simplex")
      session.execute("CREATE TABLE mytable (
        a int PRIMARY KEY,
        b boolean,
        c inet,
        d timestamp,
        e timeuuid,
        f uuid
      )", consistency: :all)
      sleep(1) # wait for the change to propagate

      insert = session.prepare("INSERT INTO mytable (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)")
      session.execute(insert, 0, true, IPAddr.new('200.199.198.197'), Time.utc(2013, 12, 11, 10, 9, 8),
                                  Cassandra::Uuid.new('FE2B4360-28C6-11E2-81C1-0800200C9A66'),
                                  Cassandra::Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66'))

      row = session.execute("SELECT * FROM mytable").first

      puts "Boolean: #{row['b']}"
      puts "Inet: #{row['c']}"
      puts "Timestamp: #{row['d'].httpdate}"
      puts "Timeuuid: #{row['e']}"
      puts "Uuid: #{row['f']}"
      """
    When it is executed
    Then its output should contain:
      """
      Boolean: true
      Inet: 200.199.198.197
      Timestamp: Wed, 11 Dec 2013 10:09:08 GMT
      Timeuuid: fe2b4360-28c6-11e2-81c1-0800200c9a66
      Uuid: 00b69180-d0e1-11e2-8b8b-0800200c9a66
      """

  Scenario: Collection-datatypes are inserted into a column family
    Given the following example:
    """ruby
      require 'cassandra'
      require 'time'

      cluster = Cassandra.connect
      at_exit { cluster.close }

      session = cluster.connect("simplex")
      session.execute("CREATE TABLE user (
        id int PRIMARY KEY,
        user_name text,
        logins List<timestamp>,
        locations Map<timestamp, double>,
        ip_addresses Set<inet>
      )", consistency: :all)
      sleep(1) # wait for the change to propagate

      insert = session.prepare("INSERT INTO user (id, user_name, logins, locations, ip_addresses) VALUES (?, ?, ?, ?, ?)")
      session.execute(insert, 0, "cassandra_user",
                                 [Time.utc(2014, 9, 11, 10, 9, 8), Time.utc(2014, 9, 12, 10, 9, 0)],
                                 {Time.utc(2014, 9, 11, 10, 9, 8) => 37.397357},
                                 Set.new([IPAddr.new('200.199.198.197'), IPAddr.new('192.168.1.15')])
                                 )

      row = session.execute("SELECT * FROM user").first

      puts "Username: #{row['user_name']}"
      puts "Logins: #{row['logins'].map(&:httpdate)}"
      puts "Location at #{Time.utc(2014, 9, 11, 10, 9, 8).httpdate}: #{row['locations'][Time.utc(2014, 9, 11, 10, 9, 8)]}"
      puts "Ip Addresses: #{row['ip_addresses'].inspect}"
      """
    When it is executed
    Then its output should contain:
      """
      Username: cassandra_user
      Logins: ["Thu, 11 Sep 2014 10:09:08 GMT", "Fri, 12 Sep 2014 10:09:00 GMT"]
      Location at Thu, 11 Sep 2014 10:09:08 GMT: 37.397357
      Ip Addresses: #<Set: {#<IPAddr: IPv4:192.168.1.15/255.255.255.255>, #<IPAddr: IPv4:200.199.198.197/255.255.255.255>}>
      """
