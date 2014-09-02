# encoding: utf-8

Given(/^a running cassandra cluster with a keyspace "(.*?)" and (a|an empty) table "(.*?)"$/) do |keyspace, empty, table|
  step "a running cassandra cluster"
  step "a keyspace \"#{keyspace}\""
  step "#{empty} table \"#{table}\""
end

Given(/^a running cassandra cluster with a keyspace "(.*?)"$/) do |keyspace|
  step "a running cassandra cluster"
  step "a keyspace \"#{keyspace}\""
end

Given(/^a running cassandra cluster$/) do
  step "a running cassandra cluster in 1 datacenter with 3 nodes in each"
end

Given(/^a running cassandra cluster with authentication enabled$/) do
  step "a running cassandra cluster"
  @username, @password = @cluster.enable_authentication
end

Given(/^a running cassandra cluster in (\d+) datacenter(?:s)? with (\d+) nodes in each$/) do |no_dc, no_nodes_per_dc|
  @cluster = setup_cluster(no_dc.to_i, no_nodes_per_dc.to_i)
end

Given(/^a keyspace "(.*?)"$/) do |keyspace|
  @cluster.create_keyspace(keyspace)
  @cluster.use_keyspace(keyspace)
end

Given(/^a(?:n)? (empty )?table "(.*?)"$/) do |empty, table|
  @cluster.create_table(table)
  @cluster.populate_table(table) unless empty
end

Given(/^the following example:$/) do |code|
  step 'a file named "example.rb" with:', prepend_encoding(code)
end

When(/^it is executed$/) do
  step 'I run `ruby -I. -rbundler/setup example.rb`'
end

Then(/^its output should contain:$/) do |output|
  step 'the output should contain:', output
end

Then(/^its output should match:$/) do |output|
  step 'the output should match:', output
end

Given(/^I wait for its output to contain "(.*?)"$/) do |output|
  step "I wait for output to contain \"#{output}\""
end

When(/^node (\d+) starts$/) do |i|
  @cluster.start_nth_node(i)
end

When(/^node (\d+) stops$/) do |i|
  @cluster.stop_node(i)
end

Given(/^node (\d+) is stopped$/) do |i|
  step "node #{i} stops"
end

When(/^node (\d+) joins$/) do |i|
  @cluster.add_node(i)
  step "node #{i} starts"
end

When(/^node (\d+) gets decommissioned$/) do |i|
  @cluster.decommission_node(i)
end

Given(/^all nodes are down$/) do
  step 'all nodes go down'
end

When(/^all nodes go down$/) do
  @cluster.stop
end

When(/^node (\d+) leaves$/) do |i|
  step "node #{i} gets decommissioned"
  step "node #{i} stops"
  @cluster.remove_node(i)
end

When(/^node (\d+) restarts$/) do |i|
  step "node #{i} stops"
  step "node #{i} starts"
end

When(/^keyspace "(.*?)" is created$/) do |keyspace|
  step "a keyspace \"#{keyspace}\""
end

When(/^keyspace "(.*?)" is dropped$/) do |keyspace|
  step "no keyspace \"#{keyspace}\""
end

Given(/^no keyspace "(.*?)"$/) do |keyspace|
  @cluster.drop_keyspace(keyspace)
end

When(/^I wait for (\d+) seconds$/) do |interval|
  sleep(interval.to_i)
end

After('@auth') do
  @cluster.disable_authentication
  @cluster.restart
end

def prepend_encoding(code)
  <<-CODE
# encoding: utf-8

unless ENV['COVERAGE'] == 'no' || RUBY_ENGINE == 'rbx'
  require 'simplecov'

  SimpleCov.root '#{Dir.pwd}'

  load '#{File.join(Dir.pwd, '.simplecov')}'

  SimpleCov.start do
    command_name 'Cucumber Example #{rand(100000)}'
  end
end

#{code}
  CODE
end