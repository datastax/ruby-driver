# encoding: utf-8

Given(/^a running cassandra cluster$/) do
  step "a running cassandra cluster in 1 datacenter with 3 nodes in each"
end

Given(/^a running cassandra cluster with authentication enabled$/) do
  step "a running cassandra cluster"
  @username, @password = @cluster.enable_authentication
end

Given(/^a running cassandra cluster with SSL encryption enabled$/) do
  step "a running cassandra cluster"
  @server_cert = @cluster.enable_ssl
end

Given(/^a running cassandra cluster with SSL client authentication enabled$/) do
  step "a running cassandra cluster"
  @server_cert, @client_cert, @private_key, @passphrase = @cluster.enable_ssl_client_auth
end

Given(/^a running cassandra cluster in (\d+) datacenter(?:s)? with (\d+) nodes in each$/) do |no_dc, no_nodes_per_dc|
  @cluster = CCM.setup_cluster(no_dc.to_i, no_nodes_per_dc.to_i)
end

Given(/^a running cassandra cluster with schema:$/) do |schema|
  step 'a running cassandra cluster'
  step 'the following schema:', schema
end

Given(/^the following schema:$/) do |schema|
  @cluster.setup_schema(schema)
end

Given(/^an empty schema$/) do
  step 'the following schema:', ''
end

When(/^I execute the following cql:$/) do |cql|
  @cluster.execute(cql)
end

Given(/^the following example:$/) do |code|
  step 'a file named "example.rb" with:', prepare_code_fragment(code)
end

When(/^it is executed$/) do
  step 'I run `ruby -I. -rbundler/setup example.rb`'
end

Then(/^its output should contain:$/) do |output|
  step 'the output should contain:', output
end

Then(/^its output should not contain:$/) do |output|
  step 'the output should not contain:', output
end

Then(/^its output should match:$/) do |output|
  step 'the output should match:', output
end

Given(/^I wait for its output to contain "(.*?)"$/) do |expected|
  Timeout.timeout(aruba.config.exit_timeout) do
    loop do
      begin
        expected = unescape_text(expected)
        expected = extract_text(expected) if !aruba.config.keep_ansi || aruba.config.remove_ansi_escape_sequences

        expect(last_command_started.output).to match(Regexp.new(expected))
      rescue RSpec::Expectations::ExpectationNotMetError => e
        retry
      end

      break
    end
  end
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

When(/^node (\d+) is unreachable$/) do |i|
  @cluster.block_node("node#{i}")
end

When(/^all nodes are unreachable$/) do
  @cluster.block_nodes
end

When(/^I wait for (\d+) seconds$/) do |interval|
  sleep(interval.to_i)
end

def prepare_code_fragment(code)
  <<-CODE
# encoding: utf-8

require 'stringio'
require 'logger'

require 'bundler/setup'
require 'cassandra'

debug_log = StringIO.new

Cassandra::Driver.let(:logger) do
  logger = Logger.new(debug_log)
  logger.level = Logger::DEBUG
  logger.formatter = proc { |s, t, _, m| "\#{t.strftime("%T,%L")} | [\#{s}] \#{m}\\n" }
  logger
end

at_exit do
  $stderr.puts("\n\n--\nDriver logs:\n\n")
  $stderr.write(debug_log.string)
end

#{code}
  CODE
end
