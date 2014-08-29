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
      )")

      insert = session.prepare("INSERT INTO mytable (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)")
      session.execute(insert, 0, 'ascii', "blob", 'text', 'varchar')

      rows = session.execute("SELECT * FROM mytable").first
      rows.keys.sort.each do |cell_name|
        puts rows[cell_name]
      end

      """
    When it is executed
    Then its output should contain:
      """
      0
      ascii
      blob
      text
      varchar
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
      )")

      insert = session.prepare("INSERT INTO mytable (a, b, c, d, e, f, g) VALUES (?, ?, ?, ?, ?, ?, ?)")
      session.execute(insert, 0, 765438000, BigDecimal.new('1313123123.234234234234234234123'),
                                  Math::PI, 3.14, 4, 67890656781923123918798273492834712837198237)

      rows = session.execute("SELECT * FROM mytable").first
      rows.keys.sort.each do |cell_name|
        puts rows[cell_name]
      end

      """
    When it is executed
    Then its output should contain:
      """
      0
      765438000
      0.1313123123234234234234234234123E10
      3.141592653589793
      3.140000104904175
      4
      67890656781923123918798273492834712837198237
      """

  Scenario: ID-like datatypes are inserted into a column family
    Given the following example:
    """ruby
      require 'cassandra'

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
      )")

      insert = session.prepare("INSERT INTO mytable (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)")
      session.execute(insert, 0, true, IPAddr.new('200.199.198.197'), Time.utc(2013, 12, 11, 10, 9, 8),
                                  Cassandra::Uuid.new('FE2B4360-28C6-11E2-81C1-0800200C9A66'),
                                  Cassandra::Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66'))

      rows = session.execute("SELECT * FROM mytable").first
      rows.keys.sort.each do |cell_name|
        puts rows[cell_name]
      end

      """
    When it is executed
    Then its output should contain:
      """
      0
      true
      200.199.198.197
      2013-12-11 02:09:08 -0800
      fe2b4360-28c6-11e2-81c1-0800200c9a66
      00b69180-d0e1-11e2-8b8b-0800200c9a66
      """

  Scenario: Collection-datatypes are inserted into a column family
    Given the following example:
    """ruby
      require 'cassandra'

      cluster = Cassandra.connect
      at_exit { cluster.close }

      session = cluster.connect("simplex")
      session.execute("CREATE TABLE user (
        id int PRIMARY KEY,
        user_name text,
        logins List<timestamp>,
        locations Map<timestamp, double>,
        ip_addresses Set<inet>
      )")

      insert = session.prepare("INSERT INTO user (id, user_name, logins, locations, ip_addresses) VALUES (?, ?, ?, ?, ?)")
      session.execute(insert, 0, "cassandra_user",
                                 [Time.utc(2014, 9, 11, 10, 9, 8), Time.utc(2014, 9, 12, 10, 9, 0)],
                                 {Time.utc(2014, 9, 11, 10, 9, 8) => 37.397357},
                                 Set.new([IPAddr.new('200.199.198.197'), IPAddr.new('192.168.1.15')])
                                 )

      rows = session.execute("SELECT * FROM user").first
      rows.keys.sort.each do |cell_name|
        p rows[cell_name]
      end

      """
    When it is executed
    Then its output should contain:
      """
      0
      #<Set: {#<IPAddr: IPv4:192.168.1.15/255.255.255.255>, #<IPAddr: IPv4:200.199.198.197/255.255.255.255>}>
      {2014-09-11 03:09:08 -0700=>37.397357}
      [2014-09-11 03:09:08 -0700, 2014-09-12 03:09:00 -0700]
      "cassandra_user"
      """