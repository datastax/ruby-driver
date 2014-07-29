# encoding: utf-8

require_relative 'benchmark'
require 'cql'

# TODO Seems to be synchronous queries only ?

class UnpreparedSelectRubyDriver < Benchmark
    def connect_to_cluster
      puts "#{Time.now - start} Connecting to cluster..."
      cluster = Cql.cluster.with_contact_points('127.0.0.1').build
      @session = cluster.connect("simplex")
      @statement  = @session.prepare('SELECT COUNT(*) FROM songs')
    end

    def target
        # Create and consume select requests
        puts "#{Time.now - start} Executing #{@iterations} selects..."
        @iterations.times.map do | result |
            if @session.execute(@statement).empty?
              increment_errors
            else
              increment_success
            end
        end
    end
end

UnpreparedSelectRubyDriver.new.run ARGV[0]
