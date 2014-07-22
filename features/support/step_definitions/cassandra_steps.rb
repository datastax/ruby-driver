# encoding: utf-8

Given(/^a running cassandra cluster with a schema "(.*?)" and (a|an empty) table "(.*?)"$/) do |schema, a, table|
  step "a running cassandra cluster"
  step "a schema \"#{schema}\""
  step "#{a} table \"#{table}\""
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

Given(/^a schema "(.*?)"$/) do |schema|
  @cluster.create_schema(schema)
  @cluster.use_schema(schema)
end

Given(/^a(?:n)? (empty )?table "(.*?)"$/) do |empty, table|
  @cluster.create_table(table)
  if empty.nil?
    @cluster.populate_table(table)
  end
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

When(/^node (\d+) starts$/) do |i|
  @cluster.start_node(i)
end

When(/^node (\d+) stops$/) do |i|
  @cluster.stop_node(i)
end

When(/^node (\d+) joins$/) do |i|
  @cluster.add_node(i)
  step "node #{i} starts"
end

When(/^node (\d+) gets decommissioned$/) do |i|
  @cluster.decommission_node(i)
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
  @cluster.create_schema(keyspace)
end

When(/^keyspace "(.*?)" is dropped$/) do |keyspace|
  @cluster.drop_schema(keyspace)
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