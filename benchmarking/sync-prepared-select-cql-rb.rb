# encoding: utf-8

# Copyright 2013-2014 DataStax, Inc.
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
