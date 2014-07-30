# encoding: utf-8

require_relative 'benchmark'
require 'cql'

class PreparedSelectCqlRb < Benchmark

    def connect_to_cluster
        puts "#{Time.now - start} Connecting to cluster..."
        client = Cql::Client::SynchronousClient.new(Cql::Client::AsynchronousClient.new(hosts: ['127.0.0.1']))
        client.connect
        client.use('simplex')
        @statement = client.prepare('SELECT COUNT(*) FROM songs')
        at_exit { client.close }
    end

    def target
        puts "#{Time.now - start} Executing #{@iterations} selects..."
        @iterations.times.map do
            begin
                @statement.execute
                increment_success
            rescue => e
                puts "#{e.class.name}: #{e.message}"
                increment_errors
            end
        end
    end
end

PreparedSelectCqlRb.new.run ARGV[0]
