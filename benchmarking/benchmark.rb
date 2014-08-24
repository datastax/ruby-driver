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

class Benchmark
    def initialize
        @success, @errors = 0, 0
        @watcher_should_continue = true
        @iterations = 10000 
    end

    def run(n)
        @iterations = n.to_i unless n.nil?
        setup
        @start = Time.now
        connect_to_cluster
        start_watcher_thread
        target
        stop_watcher_thread
        puts "#{Time.now - @start} Done."
    end

    def setup
    end

    def connect_to_cluster
        puts "Should connect to cluster"
    end

    def start_watcher_thread
        @watcher = Thread.new do
            puts "# Started watcher"
            old_ack = 0
            while @watcher_should_continue
                ack = @success + @errors
                delta = (ack - old_ack).to_s.reverse.gsub(/...(?=.)/,'\&,').reverse
                old_ack = ack
                puts "# #{delta} new queries answered (#{ack}/#{@iterations})"
                sleep 1
            end
        end
    end

    def target
        puts "Should do something #{@iterations} times"
    end

    def stop_watcher_thread
        puts "#{Time.now - @start} Cleaning..."
        @watcher_should_continue = false
        @watcher.join
        puts "# Success: #{@success}"
        puts "# Errors:  #{@errors}"
    end

    def start
        @start
    end

    def increment_success
        @success += 1
    end

    def increment_errors
        @errors += 1
    end
end
