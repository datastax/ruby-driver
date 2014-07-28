# encoding: utf-8

require 'cql'
start = Time.now

cluster   = Cql.cluster.with_contact_points('127.0.0.1').build

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
session   = cluster.connect("simplex")
statement = Cql::Statements::Simple.new("SELECT * FROM songs")
futures = total.times.map {
    session.execute_async(statement)
}

# Requests consumer
puts "#{Time.now - start} Starting consuming selects..."
futures.each do |future|
    begin
        future.get
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
