# encoding: utf-8

require_relative 'benchmark'
require 'cql'


class UnpreparedInsertCqlRb < Benchmark
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
        @client = Cql::Client::AsynchronousClient.new(hosts: ['127.0.0.1'])
        @client.connect.value
        @client.use('simplex').value
        @client.execute("TRUNCATE songs").value
        at_exit { @client.close.value }
    end

    def target
        puts "#{Time.now - start} Starting producing #{@iterations} inserts..."
        futures = @iterations.times.map do
            @client.execute("INSERT INTO songs (id, title, album, artist, tags) VALUES (?, 'Dummy song-id', 'Track 1', 'Unknown Artist', {'soundtrack', '1985'});", Cql::Uuid.new(@uuids.pop))
        end

        puts "#{Time.now - start} Starting consuming inserts..."
        futures.each do |future|
            begin
                future.value
                increment_success
            rescue => e
                puts "#{e.class.name}: #{e.message}"
                increment_errors
            end
        end
    end
end

UnpreparedInsertCqlRb.new.run ARGV[0]
