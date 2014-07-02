# encoding: utf-8

require 'cql'
require 'logger'

client = Cql::Client::AsynchronousClient.new(hosts: ['127.0.0.1'], logger: Logger.new($stderr))

client.connect.value

at_exit { client.close.value }

client.use('simplex').value
futures = 10000.times.map { client.execute("SELECT * FROM songs") }
success = 0
errors  = 0

futures.each do |future|
  begin
    success += 1
  rescue => e
    puts "#{e.class.name}: #{e.message}"
    errors += 1
  end
end

puts "success: #{success}"
puts "errors:  #{errors}"
