# encoding: utf-8

require_relative 'benchmark'
require 'cql'

class PreparedInsertCqlRb < Benchmark
    def setup
      # We do not want SecureRandom.uuid to be included in the measurements so let's generate a lot of UUID here
      # Note : Creating a Cql::Uuid from a string is included in the measured loop as it is not a Ruby API
      @uuids = Array.new
      @iterations.times do
        @uuids.push(SecureRandom.uuid)
      end
    end

    def connect_to_cluster
        puts "#{Time.now - start} Connecting to cluster..."
        client = Cql::Client::SynchronousClient.new(Cql::Client::AsynchronousClient.new(hosts: ['127.0.0.1']))
        client.connect
        client.use('simplex')
        client.execute("TRUNCATE songs")
        @statement  = client.prepare("INSERT INTO songs (id, title, album, artist, tags) VALUES (?, 'Dummy song-id', 'Track 1', 'Unknown Artist', {'soundtrack', '1985'});")
        at_exit { client.close }
    end

    def target
        puts "#{Time.now - start} Executing #{@iterations} inserts..."
        @iterations.times.map do
            begin
                @statement.execute(Cql::Uuid.new(@uuids.pop))
                increment_success
            rescue => e
                puts "#{e.class.name}: #{e.message}"
                increment_errors
            end
        end
    end
end

PreparedInsertCqlRb.new.run ARGV[0]
