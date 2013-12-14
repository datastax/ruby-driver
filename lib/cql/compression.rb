# encoding: utf-8

module Cql
  module Compression
    CompressionError = Class.new(CqlError)

    # @note Compressors given to {Cql::Client.connect} as the `:compressor`
    #   option don't need to be subclasses of this class, but need to
    #   implement the same methods. This class exists only for documentation
    #   purposes.
    class Compressor
      # @!method algorithm
      #
      # Returns the name of the algorithm this compressor supports,
      # e.g. "snappy" or "lz4".
      #
      # @return [String]

      # @!method compress?
      #
      # Before compressing a frame the compressor will be asked if it wants
      # to compress it or not. One reason it could say no is if the frame is
      # small enough that compression would be unlikely to decrease its size.
      #
      # If your operations consist mostly of small prepared statement
      # executions it might not be useful to compress the frames being sent
      # _to_ Cassandra, but enabling compression can still be useful on the
      # frames coming _from_ Cassandra. Making this method always return
      # false disables request compression, but will still make the client
      # tell Cassandra that it supports compressed frames.
      #
      # The bytes given to {#compress?} are the same as to {#compress}
      #
      # @param [String] frame the bytes of the frame to be compressed
      # @return [true, false]

      # @!method compress
      #
      # Compresses the raw bytes of a frame.
      #
      # @param [String] frame the bytes of the frame to be compressed
      # @return [String] the compressed frame

      # @!method decompress
      #
      # Decompresses the raw bytes of a compressed frame.
      #
      # @param [String] compressed_frame the bytes of the compressed
      #   frame to be uncompressed
      # @return [String]
    end
  end
end