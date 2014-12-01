Feature: Result paging

  Cassandra native protocol v2, used by Apache Cassandra 2.0, allows paging
  through query results.

  Page size can be specified by passing `:page_size` option to
  [`Cassandra::Session#execute`](/api/session#execute-instance_method).

  Once a [`Cassandra::Result`](/api/result) has been received, it can be paged
  through using `Cassandra::Result#next_page` or
  `Cassandra::Result#next_page_async` methods for synchronous and asynchronous
  next page retrieval accordingly.

  Background:
    Given a running cassandra cluster with schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      USE simplex;
      CREATE TABLE test (k text, v int, PRIMARY KEY (k, v));
      INSERT INTO test (k, v) VALUES ('a', 0);
      INSERT INTO test (k, v) VALUES ('b', 1);
      INSERT INTO test (k, v) VALUES ('c', 2);
      INSERT INTO test (k, v) VALUES ('d', 3);
      INSERT INTO test (k, v) VALUES ('e', 4);
      INSERT INTO test (k, v) VALUES ('f', 5);
      INSERT INTO test (k, v) VALUES ('g', 6);
      INSERT INTO test (k, v) VALUES ('h', 7);
      INSERT INTO test (k, v) VALUES ('i', 8);
      INSERT INTO test (k, v) VALUES ('j', 9);
      INSERT INTO test (k, v) VALUES ('k', 10);
      INSERT INTO test (k, v) VALUES ('l', 11);
      INSERT INTO test (k, v) VALUES ('m', 12);
      """

  @cassandra-version-specific @cassandra-version-2.0
  Scenario: Paging through results synchronously
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")
      result  = session.execute("SELECT * FROM test", page_size: 5)

      loop do
        puts "last page? #{result.last_page?}"
        puts "page size: #{result.size}"

        result.each do |row|
          puts row
        end
        puts ""

        break if result.last_page?
        result = result.next_page
      end

      """
    When it is executed
    Then its output should contain:
      """
      last page? false
      page size: 5
      {"k"=>"a", "v"=>0}
      {"k"=>"c", "v"=>2}
      {"k"=>"m", "v"=>12}
      {"k"=>"f", "v"=>5}
      {"k"=>"g", "v"=>6}

      last page? false
      page size: 5
      {"k"=>"e", "v"=>4}
      {"k"=>"d", "v"=>3}
      {"k"=>"h", "v"=>7}
      {"k"=>"l", "v"=>11}
      {"k"=>"j", "v"=>9}

      last page? true
      page size: 3
      {"k"=>"i", "v"=>8}
      {"k"=>"k", "v"=>10}
      {"k"=>"b", "v"=>1}

      """

  @cassandra-version-specific @cassandra-version-2.0
  Scenario: Paging through results asynchronously
    Given the following example:
      """ruby
      require 'cassandra'

      def page_through(future)
        future.then do |result|
          puts "last page? #{result.last_page?}"
          puts "page size: #{result.size}"

          result.each do |row|
            puts row
          end
          puts ""

          page_through(result.next_page_async) unless result.last_page?
        end
      end

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")
      select  = session.prepare("SELECT * FROM test")
      future  = session.execute_async(select, page_size: 5)

      page_through(future).join
      """
    When it is executed
    Then its output should contain:
      """
      last page? false
      page size: 5
      {"k"=>"a", "v"=>0}
      {"k"=>"c", "v"=>2}
      {"k"=>"m", "v"=>12}
      {"k"=>"f", "v"=>5}
      {"k"=>"g", "v"=>6}

      last page? false
      page size: 5
      {"k"=>"e", "v"=>4}
      {"k"=>"d", "v"=>3}
      {"k"=>"h", "v"=>7}
      {"k"=>"l", "v"=>11}
      {"k"=>"j", "v"=>9}

      last page? true
      page size: 3
      {"k"=>"i", "v"=>8}
      {"k"=>"k", "v"=>10}
      {"k"=>"b", "v"=>1}

      """

  @cassandra-version-specific @cassandra-version-2.0
  Scenario: Using paging state for stateless paging
    Given the following example:
      """ruby
      require 'cassandra'

      cluster      = Cassandra.cluster
      session      = cluster.connect("simplex")
      paging_state = nil

      loop do
        result = session.execute("SELECT * FROM test", page_size: 5, paging_state: paging_state)
        puts "last page? #{result.last_page?}"
        puts "page size: #{result.size}"

        result.each do |row|
          puts row
        end
        puts ""

        break if result.last_page?
        paging_state = result.paging_state
      end

      """
    When it is executed
    Then its output should contain:
      """
      last page? false
      page size: 5
      {"k"=>"a", "v"=>0}
      {"k"=>"c", "v"=>2}
      {"k"=>"m", "v"=>12}
      {"k"=>"f", "v"=>5}
      {"k"=>"g", "v"=>6}

      last page? false
      page size: 5
      {"k"=>"e", "v"=>4}
      {"k"=>"d", "v"=>3}
      {"k"=>"h", "v"=>7}
      {"k"=>"l", "v"=>11}
      {"k"=>"j", "v"=>9}

      last page? true
      page size: 3
      {"k"=>"i", "v"=>8}
      {"k"=>"k", "v"=>10}
      {"k"=>"b", "v"=>1}

      """
