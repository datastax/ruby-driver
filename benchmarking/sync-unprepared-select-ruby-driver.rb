# encoding: utf-8

require_relative 'benchmark'
require 'cql'

class UnpreparedSelectRubyDriver < Benchmark
    def connect_to_cluster
        puts "#{Time.now - start} Connecting to cluster..."
        @cluster = Cql.connect(hosts: ['127.0.0.1'])
        @session = @cluster.connect("simplex")
    end

    def target
        puts "#{Time.now - start} Executing #{@iterations} selects..."
        statement = Cql::Statements::Simple.new("SELECT COUNT(*) FROM songs")
        @iterations.times.map do
            begin
                @session.execute(statement)
                increment_success
            rescue => e
                puts "#{e.class.name}: #{e.message}"
                increment_errors
            end
        end
    end
end

UnpreparedSelectRubyDriver.new.run ARGV[0]
