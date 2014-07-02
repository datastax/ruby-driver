CREATE TABLE playlists (
  id uuid,
  title text,
  album text,
  artist text,
  song_id uuid,
  PRIMARY KEY (id, title, album, artist)
);
