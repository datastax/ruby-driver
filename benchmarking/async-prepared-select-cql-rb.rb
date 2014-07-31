# encoding: utf-8

require_relative 'benchmark'
require 'cql'

class PreparedSelectCqlRb < Benchmark

    def connect_to_cluster
        puts "#{Time.now - start} Connecting to cluster..."
        client = Cql::Client::AsynchronousClient.new(hosts: ['127.0.0.1'])
        client.connect.value
        client.use('simplex').value
        @statement = client.prepare('SELECT * FROM songs').value
        at_exit { client.close.value }
    end

    def target
        puts "#{Time.now - start} Starting producing #{@iterations} selects..."
        futures = @iterations.times.map do
            @statement.execute
        end

        puts "#{Time.now - start} Starting consuming selects..."
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

PreparedSelectCqlRb.new.run ARGV[0]
