# encoding: utf-8

require_relative 'benchmark'
require 'cql'

class PreparedSelectRubyDriver < Benchmark
    def connect_to_cluster
        puts "#{Time.now - start} Connecting to cluster..."
        @cluster = Cql.cluster.with_contact_points('127.0.0.1').build
        @session = @cluster.connect("simplex")
        @statement  = @session.prepare('SELECT COUNT(*) FROM songs')
    end

    def target
        puts "#{Time.now - start} Executing #{@iterations} selects..."
        futures = @iterations.times.map do
            @session.execute_async(@statement)
        end

        puts "#{Time.now - start} Starting consuming selects..."
        futures.each do |future|
            begin
                future.get
                increment_success
            rescue => e
                puts "#{e.class.name}: #{e.message}"
                increment_errors
            end
        end
    end
end

PreparedSelectRubyDriver.new.run ARGV[0]
