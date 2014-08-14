Feature: Preparing statements asynchronously

  Session objects can be used to prepare a statement asynchronously using `Cql::Session#prepare_async` method.
  This method returns a `Cql::Future<Cql::Statements::Prepared>` instance.

  Background:
    Given a running cassandra cluster with a keyspace "simplex" and a table "songs"
    And a table "playlists"

  Scenario: Preparing statements in parallel
    Given the following example:
      """ruby
      require 'cql'
      
      cluster = Cql.cluster.build
      session = cluster.connect("simplex")
      
      # prepare 2 statements in parallel
      select_song_future      = session.prepare_async("SELECT * FROM songs WHERE id = ?")
      select_playlist_future  = session.prepare_async("SELECT * FROM playlists WHERE id = ?")
      
      # get prepared statements
      select_song     = select_song_future.get
      select_playlist = select_playlist_future.get
      
      # execute prepared statements
      song_id  = Cql::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50')
      song     = session.execute(select_song, song_id).first

      puts "#{song["artist"]}: #{song["title"]} / #{song["album"]} has id #{song_id}"

      playlist_id = Cql::Uuid.new('2cc9ccb7-6221-4ccb-8387-f22b6a1b354d')
      playlist    = session.execute(select_playlist, playlist_id)
      
      puts "Playlist #{playlist_id} has #{playlist.size} songs"
      playlist.each do |song|
        puts "#{song["artist"]}: #{song["title"]} / #{song["album"]}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Joséphine Baker: La Petite Tonkinoise / Bye Bye Blackbird has id 756716f7-2e54-4715-9f00-91dcbea6cf50
      Playlist 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d has 2 songs
      """
