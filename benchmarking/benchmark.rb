# encoding: utf-8

class Benchmark
    def initialize
        @success, @errors = 0, 0
        @watcher_should_continue = true
        @iterations = 10000 
    end

    def run(n)
        @start = Time.now
        @iterations = n.to_i unless n.nil?
        connect_to_cluster
        start_watcher_thread
        target
        stop_watcher_thread
        puts "#{Time.now - @start} Done."
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
