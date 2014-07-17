# encoding: utf-8

Given(/^a cassandra cluster with schema "(.*?)" with (an empty )?table "(.*?)"$/) do |schema, empty, table|
  step "a running cassandra cluster"
  step "schema \"#{schema}\""

  if empty.nil?
    step "table \"#{table}\" with data"
  else
    step "table \"#{table}\""
  end
end

Given(/^a running cassandra cluster$/) do
  @cluster = setup_cluster
end

Given(/^a running cassandra cluster with authentication enabled$/) do
  step "a running cassandra cluster"
  @username, @password = @cluster.setup_authentication
end

Given(/^a (\d+)x(\d+)-nodes running cassandra cluster$/) do |no_dc, no_nodes_per_dc|
  @cluster = setup_cluster(no_dc.to_i, no_nodes_per_dc.to_i)
end

Given(/^a (\d+)x(\d+)-nodes running cassandra cluster with schema "(.*?)" with (an empty )?table "(.*?)"$/) do |no_dc, no_nodes_per_dc, schema, empty, table|
  step "a #{no_dc}x#{no_nodes_per_dc}-nodes running cassandra cluster"
  step "schema \"#{schema}\""

  if empty.nil?
    step "table \"#{table}\" with data"
  else
    step "table \"#{table}\""
  end
end

Given(/^schema "(.*?)"$/) do |schema|
  @cluster.create_schema(schema)
  @cluster.use_schema(schema)
end

Given(/^table "(.*?)"$/) do |table|
  @cluster.create_table(table)
end

Given(/^table "(.*?)" with data$/) do |table|
  step "table \"#{table}\""
  @cluster.populate_table(table)
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

def prepend_encoding(code)
  <<-CODE
# encoding: utf-8

#{code}
  CODE
end