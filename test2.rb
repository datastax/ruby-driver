# encoding: utf-8

require 'cql'
# require 'allocation_stats'

cluster = Cql.connect(hosts: ['127.0.0.1'])

# at_exit { cluster.close }

session   = cluster.connect("simplex")
statement = Cql::Statements::Simple.new("SELECT * FROM songs")

# stats = AllocationStats.trace.trace do
  futures = 10000.times.map { session.execute_async(statement) }
  success = 0
  errors  = 0

  futures.each do |future|
    begin
      future.get
      success += 1
    rescue Cql::Errors::NoHostsAvailable => e
      raise e.errors.first.last
    rescue => e
      puts "#{e.class.name}: #{e.message}"
      puts e.backtrace
      errors += 1
    end
  end

  puts "success: #{success}"
  puts "errors:  #{errors}"
# end
#
# puts stats.allocations(alias_paths: true).group_by(:sourcefile, :sourceline, :class).to_text
