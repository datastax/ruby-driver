# encoding: utf-8

require_relative 'benchmark'
require 'cql'

class UnpreparedSelectCqlRb < Benchmark
    def connect_to_cluster
        puts "#{Time.now - start} Connecting to cluster..."
        @client = Cql::Client::AsynchronousClient.new(hosts: ['127.0.0.1'])
        @client.connect.value
        @client.use('simplex').value
        at_exit { @client.close.value }
    end

    def target
        puts "#{Time.now - start} Starting producing #{@iterations} selects..."
        futures = @iterations.times.map do
            @client.execute("SELECT * FROM songs")
        end

        puts "#{Time.now - start} Starting consuming selects..."
        futures.each do |future|
            begin
                future.value.size
                increment_success
            rescue => e
                puts "#{e.class.name}: #{e.message}"
                increment_errors
            end
        end
    end
end

UnpreparedSelectCqlRb.new.run ARGV[0]
