# encoding: utf-8

class Benchmark
    def run(n = 10000)
        @start = Time.now
        @iterations = n
        connect_to_cluster
        start_watcher_thread
        target(@iterations)
        stop_watcher_thread
        puts "#{Time.now - @start} Done."
    end

    def connect_to_cluster
        puts "Should connect to cluster"
    end

    def start_watcher_thread
        @watcher_should_continue = true
        @success, @errors = 0, 0
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

    def target(iterations)
        puts "Should do something #{iterations} times"
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

    def increment errors
        @errors += 1
    end
end
