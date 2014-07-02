# encoding: utf-8

Feature: prepared statements for writes

  Prepared statements are used to prepare a write query only once and execute
  it multiple times with different values. A bind variable marker "?" is used
  to represent a dynamic value in a statement.

  Background:
    Given a cassandra cluster with schema "simplex" with an empty table "playlists"

  Scenario: an INSERT statement is prepared
    Given the following example:
      """ruby
      require 'cql'

      cluster = Cql.builder \
                  .add_contact_point("127.0.0.1") \
                  .build

      at_exit { cluster.close }

      session = cluster.connect("simplex")
      insert  = session.prepare(
                  "INSERT INTO playlists (id, song_id, title, artist, album) " \
                  "VALUES (62c36092-82a1-3a00-93d1-46196ee77204, ?, ?, ?, ?)"
                )
      songs   = [
        {
          :id     => Cql::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50'),
          :title  => 'La Petite Tonkinoise',
          :album  => 'Bye Bye Blackbird',
          :artist => 'Joséphine Baker'
        },
        {
          :id     => Cql::Uuid.new('f6071e72-48ec-4fcb-bf3e-379c8a696488'),
          :title  => 'Die Mösch',
          :album  => 'In Gold',
          :artist => 'Willi Ostermann'
        },
        {
          :id     => Cql::Uuid.new('fbdf82ed-0063-4796-9c7c-a3d4f47b4b25'),
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
