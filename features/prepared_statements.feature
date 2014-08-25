Feature: Prepared statements

  Prepared statements are used to prepare a write query only once and execute
  it multiple times with different values. A bind variable marker `?` is used
  to represent a dynamic value in a statement.

  Scenario: an INSERT statement is prepared
    Given a running cassandra cluster with a keyspace "simplex" and an empty table "playlists"
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.connect

      at_exit { cluster.close }

      session = cluster.connect("simplex")
      insert  = session.prepare(
                  "INSERT INTO playlists (id, song_id, title, artist, album) " \
                  "VALUES (62c36092-82a1-3a00-93d1-46196ee77204, ?, ?, ?, ?)"
                )
      songs   = [
        {
          :id     => Cassandra::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50'),
          :title  => 'La Petite Tonkinoise',
          :album  => 'Bye Bye Blackbird',
          :artist => 'Joséphine Baker'
        },
        {
          :id     => Cassandra::Uuid.new('f6071e72-48ec-4fcb-bf3e-379c8a696488'),
          :title  => 'Die Mösch',
          :album  => 'In Gold',
          :artist => 'Willi Ostermann'
        },
        {
          :id     => Cassandra::Uuid.new('fbdf82ed-0063-4796-9c7c-a3d4f47b4b25'),
          :title  => 'Memo From Turner',
          :album  => 'Performance',
          :artist => 'Mick Jager'
        },
      ]

      songs.each do |song|
        session.execute(insert, song[:id], song[:title], song[:artist], song[:album])
      end

      session.execute("SELECT * FROM playlists").each do |row|
        $stderr.puts("#{row["artist"]}: #{row["title"]} / #{row["album"]}")
      end
      """
    When it is executed
    Then its output should contain:
      """
      Joséphine Baker: La Petite Tonkinoise / Bye Bye Blackbird
      """
    And its output should contain:
      """
      Willi Ostermann: Die Mösch / In Gold
      """
    And its output should contain:
      """
      Mick Jager: Memo From Turner / Performance
      """

  Scenario: a SELECT statement with parameterized LIMIT is prepared
    Given a running cassandra cluster with a keyspace "simplex" and a table "playlists"
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.connect

      at_exit { cluster.close }

      session = cluster.connect("simplex")
      select  = session.prepare("SELECT * FROM playlists LIMIT ?")
      limits  = [1, 2, 3]

      limits.each do |limit|
        rows = session.execute(select, limit)
        puts "selected #{rows.size} row(s)"
      end
      """
    When it is executed
    Then its output should contain:
      """
      selected 1 row(s)
      selected 2 row(s)
      selected 3 row(s)
      """
