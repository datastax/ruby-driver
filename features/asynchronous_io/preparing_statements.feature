Feature: Preparing statements asynchronously

  Session objects can be used to prepare a statement asynchronously using `Cassandra::Session#prepare_async` method.
  This method returns a `Cassandra::Future<Cassandra::Statements::Prepared>` instance.

  Background:
    Given a running cassandra cluster with schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      USE simplex;
      CREATE TABLE songs (
        id uuid PRIMARY KEY,
        title text,
        album text,
        artist text,
        tags set<text>,
        data blob
      );
      INSERT INTO songs (id, title, album, artist, tags)
      VALUES (
         756716f7-2e54-4715-9f00-91dcbea6cf50,
         'La Petite Tonkinoise',
         'Bye Bye Blackbird',
         'Joséphine Baker',
         {'jazz', '2013'})
      ;
      INSERT INTO songs (id, title, album, artist, tags)
      VALUES (
         f6071e72-48ec-4fcb-bf3e-379c8a696488,
         'Die Mösch',
         'In Gold',
         'Willi Ostermann',
         {'kölsch', '1996', 'birds'}
      );
      INSERT INTO songs (id, title, album, artist, tags)
      VALUES (
         fbdf82ed-0063-4796-9c7c-a3d4f47b4b25,
         'Memo From Turner',
         'Performance',
         'Mick Jager',
         {'soundtrack', '1991'}
      );
      CREATE TABLE playlists (
        id uuid,
        title text,
        album text,
        artist text,
        song_id uuid,
        PRIMARY KEY (id, title, album, artist)
      );
      INSERT INTO playlists (id, song_id, title, album, artist)
      VALUES (
         2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,
         756716f7-2e54-4715-9f00-91dcbea6cf50,
         'La Petite Tonkinoise',
         'Bye Bye Blackbird',
         'Joséphine Baker'
      );
      INSERT INTO playlists (id, song_id, title, album, artist)
      VALUES (
         2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,
         f6071e72-48ec-4fcb-bf3e-379c8a696488,
         'Die Mösch',
         'In Gold',
         'Willi Ostermann'
      );
      INSERT INTO playlists (id, song_id, title, album, artist)
      VALUES (
         3fd2bedf-a8c8-455a-a462-0cd3a4353c54,
         fbdf82ed-0063-4796-9c7c-a3d4f47b4b25,
         'Memo From Turner',
         'Performance',
         'Mick Jager'
      );
      INSERT INTO playlists (id, song_id, title, album, artist)
      VALUES (
         3fd2bedf-a8c8-455a-a462-0cd3a4353c54,
         756716f7-2e54-4715-9f00-91dcbea6cf50,
         'La Petite Tonkinoise',
         'Bye Bye Blackbird',
         'Joséphine Baker'
      );
      """

  Scenario: Preparing statements in parallel
    Given the following example:
      """ruby
      require 'cassandra'
      
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")
      
      # prepare 2 statements in parallel
      select_song_future      = session.prepare_async("SELECT * FROM songs WHERE id = ?")
      select_playlist_future  = session.prepare_async("SELECT * FROM playlists WHERE id = ?")
      
      # get prepared statements
      select_song     = select_song_future.get
      select_playlist = select_playlist_future.get
      
      # execute prepared statements
      song_id  = Cassandra::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50')
      song     = session.execute(select_song, arguments: [song_id]).first

      puts "#{song["artist"]}: #{song["title"]} / #{song["album"]} has id #{song_id}"

      playlist_id = Cassandra::Uuid.new('2cc9ccb7-6221-4ccb-8387-f22b6a1b354d')
      playlist    = session.execute(select_playlist, arguments: [playlist_id])
      
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
