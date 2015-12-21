Feature: Datatypes

  Apache Cassandra [supports a variety of datatypes](http://www.datastax.com/documentation/cql/3.0/cql/cql_reference/cql_data_types_c.html).
  Ruby driver transparently maps each of those datatypes to a specific Ruby type.

  Datatypes that map to `String` can have different encodings.

  Cassandra uuid and timeuuid are represented with `Cassandra::Uuid` and
  `Cassandra::TimeUuid` accordingly.

  Background:
    Given a running cassandra cluster

  Scenario: Using strings
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TABLE simplex.mytable (
        a int PRIMARY KEY,
        b ascii,
        c blob,
        d text,
        e varchar,
      );
      INSERT INTO simplex.mytable (a, b, c, d, e)
      VALUES (
        0,
        'ascii',
        0x626c6f62,
        'text',
        'varchar'
      )
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

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

  Scenario: Using numbers
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TABLE simplex.mytable (
        a int PRIMARY KEY,
        b bigint,
        c decimal,
        d double,
        e float,
        f int,
        g varint
      );
      INSERT INTO simplex.mytable (a, b, c, d, e, f, g)
      VALUES (
        0,
        765438000,
        1313123123.234234234234234234123,
        3.141592653589793,
        3.14,
        4,
        67890656781923123918798273492834712837198237
      )
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

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

  Scenario: Using identifiers, booleans and ip addresses
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TABLE simplex.mytable (
        a int PRIMARY KEY,
        b boolean,
        c inet,
        d timestamp,
        e timeuuid,
        f uuid
      );
      INSERT INTO simplex.mytable (a, b, c, d, e, f)
      VALUES (
        0,
        true,
        '200.199.198.197',
        '2013-12-11 10:09:08+0000',
        FE2B4360-28C6-11E2-81C1-0800200C9A66,
        00b69180-d0e1-11e2-8b8b-0800200c9a66
      )
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      row = session.execute("SELECT * FROM mytable").first

      puts "Boolean: #{row['b']}"
      puts "Inet: #{row['c'].class.name} - #{row['c']}"
      puts "Timestamp: #{row['d'].httpdate}"
      puts "Timeuuid: #{row['e']}"
      puts "Uuid: #{row['f']}"
      """
    When it is executed
    Then its output should contain:
      """
      Boolean: true
      Inet: IPAddr - 200.199.198.197
      Timestamp: Wed, 11 Dec 2013 10:09:08 GMT
      Timeuuid: fe2b4360-28c6-11e2-81c1-0800200c9a66
      Uuid: 00b69180-d0e1-11e2-8b8b-0800200c9a66
      """

  Scenario: Using lists, maps and sets
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TABLE simplex.user (
        id int PRIMARY KEY,
        logins List<timestamp>,
        locations Map<timestamp, double>,
        ip_addresses Set<inet>
      );
      INSERT INTO simplex.user (id, logins, locations, ip_addresses)
      VALUES (
        0,
        ['2014-09-11 10:09:08+0000', '2014-09-12 10:09:00+0000'],
        {'2014-09-11 10:09:08+0000': 37.397357},
        {'200.199.198.197', '192.168.1.15'}
      )
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      row = session.execute("SELECT * FROM user").first

      puts "Logins: #{row['logins'].map(&:httpdate)}"
      puts "Location at #{row['locations'].first.first.httpdate}: #{row['locations'].first.last}"
      puts "Ip Addresses: #{row['ip_addresses'].inspect}"
      """
    When it is executed
    Then its output should contain:
      """
      Logins: ["Thu, 11 Sep 2014 10:09:08 GMT", "Fri, 12 Sep 2014 10:09:00 GMT"]
      Location at Thu, 11 Sep 2014 10:09:08 GMT: 37.397357
      Ip Addresses: #<Set: {#<IPAddr: IPv4:192.168.1.15/255.255.255.255>, #<IPAddr: IPv4:200.199.198.197/255.255.255.255>}>
      """

  @cassandra-version-specific @cassandra-version-2.1
  Scenario: Using tuples
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TABLE simplex.user (
        id int PRIMARY KEY,
        name frozen <tuple<varchar, varchar>>
      );
      INSERT INTO simplex.user (id, name)
      VALUES (0, ('John', 'Smith'))
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      row = session.execute("SELECT * FROM user", consistency: :all).first

      puts "Name: #{row['name']}"

      update = session.prepare("UPDATE user SET name=? WHERE id=0")
      session.execute(update, arguments: [Cassandra::Tuple.new('Jane', 'Doe')], consistency: :all)

      row = session.execute("SELECT * FROM user").first
      puts "Name: #{row['name']}"

      session.execute("INSERT INTO user (id, name) VALUES (1, (?, ?))", arguments: ['Agent', 'Smith'], consistency: :all)
      row = session.execute("SELECT * FROM user WHERE id=1").first
      puts "Name: #{row['name']}"

      insert = session.prepare("INSERT INTO user (id, name) VALUES (?, ?)")
      session.execute(insert, arguments: [2, Cassandra::Tuple.new('Apache', 'Cassandra')], consistency: :all)
      row = session.execute("SELECT * FROM user WHERE id=2").first

      puts "Name: #{row['name']}"

      begin
        session.execute(update, arguments: [Cassandra::Tuple.new('Jane', 'Doe', 'Extra')])
      rescue => e
        puts "#{e.class.name}: #{e.message}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Name: (John, Smith)
      Name: (Jane, Doe)
      Name: (Agent, Smith)
      Name: (Apache, Cassandra)
      ArgumentError: argument for "name" must be tuple<text, text>, (Jane, Doe, Extra) given
      """

  @cassandra-version-specific @cassandra-version-2.1.3
  Scenario: Using nested collections
    Given the following schema:
    """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TABLE simplex.airports (
        id int PRIMARY KEY,
        flight_destinations Map<int, frozen<Tuple<double, double>>>,
        flight_numbers List<frozen<Set<int>>>
      );
      INSERT INTO simplex.airports (id, flight_destinations, flight_numbers)
      VALUES (
        0,
        {747: (37.397357, 42.7357), 458: (122.7423, 2.92547), 638: (105.357423, 20.57925)},
        [{747, 458} , {638}]
      )
      """
    And the following example:
    """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      row = session.execute("SELECT * FROM airports").first

      puts "Flight_numbers: #{row['flight_numbers'].inspect}"
      row['flight_destinations'].each_pair do | key, value |
        puts "Flight: #{key} to destination: #{value}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Flight_numbers: [#<Set: {458, 747}>, #<Set: {638}>]
      Flight: 458 to destination: (122.7423, 2.92547)
      Flight: 638 to destination: (105.357423, 20.57925)
      Flight: 747 to destination: (37.397357, 42.7357)
      """

  @cassandra-version-specific @cassandra-version-2.2
  Scenario: Using time, date, smallint and tinyint
    Given the following schema:
    """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TABLE simplex.checks (
        id int PRIMARY KEY,
        date date,
        time time,
        attempt tinyint,
        value   smallint
      );
      INSERT INTO simplex.checks (id, date, time, attempt, value)
      VALUES (0, '2015-05-03', '16:42:23.553', 1, 452);
      INSERT INTO simplex.checks (id, date, time, attempt, value)
      VALUES (1, '2015-05-03', '16:42:33.555', 1, 458);
      INSERT INTO simplex.checks (id, date, time, attempt, value)
      VALUES (2, '2015-05-03', '16:42:53.554', 4, 150);
      INSERT INTO simplex.checks (id, date, time, attempt, value)
      VALUES (3, '2015-05-03', '16:43:03.557', 2, 225);
      """
    And the following example:
    """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      session.execute("SELECT * FROM checks").each do |row|
        puts "Value on #{row['date']} at #{row['time']} was #{row['value']}, attempted #{row['attempt']} times"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Value on 2015-05-03 at 16:42:33.555 was 458, attempted 1 times
      Value on 2015-05-03 at 16:42:23.553 was 452, attempted 1 times
      Value on 2015-05-03 at 16:42:53.554 was 150, attempted 4 times
      Value on 2015-05-03 at 16:43:03.557 was 225, attempted 2 times
      """
