# encoding: utf-8

Given(/^a running cassandra cluster$/) do
  step "a running cassandra cluster in 1 datacenter with 3 nodes in each"
end

Given(/^a running cassandra cluster with authentication enabled$/) do
  step "a running cassandra cluster"
  @username, @password = @cluster.enable_authentication
end

Given(/^a running cassandra cluster in (\d+) datacenter(?:s)? with (\d+) nodes in each$/) do |no_dc, no_nodes_per_dc|
  @cluster = CCM.setup_cluster(no_dc.to_i, no_nodes_per_dc.to_i)
end

Given(/^a running cassandra cluster with schema:$/) do |schema|
  step 'a running cassandra cluster'
  step 'the following schema:', schema
end

Given(/^the following schema:$/) do |schema|
  @current_cluster.clear
  @cluster.execute_query(schema.strip.chomp(";"))
  sleep(2)
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
  @cluster.start_node("node#{i}")
end

When(/^node (\d+) stops$/) do |i|
  @cluster.stop_node("node#{i}")
end

Given(/^node (\d+) is stopped$/) do |i|
  step "node #{i} stops"
end

When(/^node (\d+) joins$/) do |i|
  @cluster.add_node("node#{i}")
  step "node #{i} starts"
end

When(/^node (\d+) gets decommissioned$/) do |i|
  @cluster.decommission_node("node#{i}")
end

Given(/^all nodes are down$/) do
  step 'all nodes go down'
end

When(/^all nodes go down$/) do
  @cluster.stop
end

When(/^node (\d+) leaves$/) do |i|
  step "node #{i} gets decommissioned"
  @cluster.remove_node("node#{i}")
end

When(/^node (\d+) restarts$/) do |i|
  step "node #{i} stops"
  step "node #{i} starts"
end

When(/^I wait for (\d+) seconds$/) do |interval|
  sleep(interval.to_i)
end

After('@auth') do
  @cluster.disable_authentication
end

def prepend_encoding(code)
  <<-CODE
# encoding: utf-8

#{code}
  CODE
end
