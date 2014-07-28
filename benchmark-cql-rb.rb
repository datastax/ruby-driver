# encoding: utf-8

require 'cql'
start = Time.now

client = Cql::Client::AsynchronousClient.new(hosts: ['127.0.0.1'])
client.connect.value
at_exit { client.close.value }

# Parameters
total = if ARGV[0].nil?
            1000 * 10
        else
            ARGV[0].to_i
        end

# Watcher
watcher_should_continue = true
success = 0
errors  = 0
watcher = Thread.new {
    puts "# Started watcher"
    old_ack = 0
    while watcher_should_continue
        ack = success + errors
        delta = (ack - old_ack).to_s.reverse.gsub(/...(?=.)/,'\&,').reverse
        old_ack = ack
        puts "# #{delta} new queries answered (#{ack}/#{total})"
        sleep 1
    end
}

# Requests producer
puts "#{Time.now - start} Starting producing selects..."
client.use('simplex').value
futures = total.times.map {
    client.execute("SELECT * FROM songs")
}

# Requests consumer
puts "#{Time.now - start} Starting consuming selects..."
futures.each do |future|
    begin
        future.value
        success += 1
    rescue => e
        puts "#{e.class.name}: #{e.message}"
        errors += 1
    end
end

puts "#{Time.now - start} Cleaning..."

watcher_should_continue = false
watcher.join
puts "# Success: #{success}"
puts "# Errors:  #{errors}"
puts "#{Time.now - start} Done."
