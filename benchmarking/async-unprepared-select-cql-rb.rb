# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

require_relative 'benchmark'
require 'cassandra'

class UnpreparedSelectCqlRb < Benchmark
    def connect_to_cluster
        puts "#{Time.now - start} Connecting to cluster..."
        @client = Cassandra::Client::AsynchronousClient.new(hosts: ['127.0.0.1'])
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
