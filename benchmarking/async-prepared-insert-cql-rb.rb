# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
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

class PreparedInsertCqlRb < Benchmark
    def setup
      # We do not want SecureRandom.uuid to be included in the measurements so let's generate a lot of UUID here
      # Note : Creating a Cassandra::Uuid from a string is included in the measured loop as it is not a Ruby API
      @uuids = Array.new
      @iterations.times do
        @uuids.push(SecureRandom.uuid)
      end
    end

    def connect_to_cluster
        puts "#{Time.now - start} Connecting to cluster..."
        client = Cassandra::Client::AsynchronousClient.new(hosts: ['127.0.0.1'])
        client.connect.value
        client.use('simplex').value
        client.execute("TRUNCATE songs").value
        @statement  = client.prepare("INSERT INTO songs (id, title, album, artist, tags) VALUES (?, 'Dummy song-id', 'Track 1', 'Unknown Artist', {'soundtrack', '1985'});").value
        at_exit { client.close.value }
    end

    def target
        puts "#{Time.now - start} Starting producing #{@iterations} inserts..."
        futures = @iterations.times.map do
            @statement.execute(Cassandra::Uuid.new(@uuids.pop))
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

PreparedInsertCqlRb.new.run ARGV[0]
