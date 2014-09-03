Feature: Datatypes

  Query results can be paged with a specified size at query execution using `Cassandra::Session#execute`.
  The next results page can be retrieved from the current result page using `Cassandra::Result#next_page`
  for both prepared and non-prepared statements.
  Use `Cassandra::Result#next_page_async` to asynchronously retrieve the next page.

  Background:
    Given a running cassandra cluster with a keyspace "simplex"

  @cassandra-version-specific @cassandra-version-2.0
  Scenario: Non-prepared statements are executed with synchronous paging
    Given the following example:
    """ruby
      require 'cassandra'

      cluster = Cassandra.connect
      at_exit { cluster.close }

      session = cluster.connect("simplex")
      session.execute("DROP TABLE test", consistency: :all) rescue nil
      session.execute("CREATE TABLE test (k text, v int, PRIMARY KEY (k, v))", consistency: :all)
      sleep(1) # wait for the change to propagate

      session.execute("INSERT INTO test (k, v) VALUES ('a', 0)")
      session.execute("INSERT INTO test (k, v) VALUES ('b', 1)")
      session.execute("INSERT INTO test (k, v) VALUES ('c', 2)")
      session.execute("INSERT INTO test (k, v) VALUES ('d', 3)")
      session.execute("INSERT INTO test (k, v) VALUES ('e', 4)")
      session.execute("INSERT INTO test (k, v) VALUES ('f', 5)")
      session.execute("INSERT INTO test (k, v) VALUES ('g', 6)")
      session.execute("INSERT INTO test (k, v) VALUES ('h', 7)")
      session.execute("INSERT INTO test (k, v) VALUES ('i', 8)")
      session.execute("INSERT INTO test (k, v) VALUES ('j', 9)")
      session.execute("INSERT INTO test (k, v) VALUES ('k', 10)")
      session.execute("INSERT INTO test (k, v) VALUES ('l', 11)")
      session.execute("INSERT INTO test (k, v) VALUES ('m', 12)")

      result = session.execute("SELECT * FROM test", page_size: 5)

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
  Scenario: Prepared statements are executed with asynchronous paging
    Given the following example:
    """ruby
      require 'cassandra'

      cluster = Cassandra.connect
      at_exit { cluster.close }

      session = cluster.connect("simplex")
      session.execute("DROP TABLE test", consistency: :all) rescue nil
      session.execute("CREATE TABLE test (k text, v int, PRIMARY KEY (k, v))", consistency: :all)
      sleep(1) # wait for the change to propagate

      session.execute("INSERT INTO test (k, v) VALUES ('a', 0)")
      session.execute("INSERT INTO test (k, v) VALUES ('b', 1)")
      session.execute("INSERT INTO test (k, v) VALUES ('c', 2)")
      session.execute("INSERT INTO test (k, v) VALUES ('d', 3)")
      session.execute("INSERT INTO test (k, v) VALUES ('e', 4)")
      session.execute("INSERT INTO test (k, v) VALUES ('f', 5)")
      session.execute("INSERT INTO test (k, v) VALUES ('g', 6)")
      session.execute("INSERT INTO test (k, v) VALUES ('h', 7)")
      session.execute("INSERT INTO test (k, v) VALUES ('i', 8)")
      session.execute("INSERT INTO test (k, v) VALUES ('j', 9)")
      session.execute("INSERT INTO test (k, v) VALUES ('k', 10)")
      session.execute("INSERT INTO test (k, v) VALUES ('l', 11)")
      session.execute("INSERT INTO test (k, v) VALUES ('m', 12)")

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

      select = session.prepare("SELECT * FROM test")
      future = session.execute_async(select, page_size: 5)
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