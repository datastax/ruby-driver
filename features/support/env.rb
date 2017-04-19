# encoding: utf-8

require 'bundler/setup'

unless ENV['COVERAGE'] == 'no' || RUBY_ENGINE == 'rbx'
  require 'simplecov'
  require 'simplecov-cobertura'

  SimpleCov.formatter = SimpleCov::Formatter::CoberturaFormatter
  SimpleCov.start do
    command_name 'Cucumber'
  end
end

require File.dirname(__FILE__) + '/../../support/retry.rb'
require File.dirname(__FILE__) + '/../../support/ccm.rb'

if RUBY_ENGINE == 'jruby'
  ENV['JRUBY_OPTS'] ||= '-Xcli.debug=true --debug'
end

require 'aruba/cucumber'
require 'pathname'
require 'tempfile'
require 'yaml'

require 'cassandra'
require 'cassandra/compression/compressors/snappy'
require 'cassandra/compression/compressors/lz4'

if RUBY_ENGINE == 'rbx'
  class Aruba::ArubaPath
    def to_str
      to_s
    end
  end
end

AfterConfiguration do |configuration|
  slow_features = ['features/load_balancing/default_policy.feature', 'features/load_balancing/datacenter_aware.feature', 'features/load_balancing/round_robin.feature', 'features/load_balancing/token_aware.feature']

  features_files = configuration.feature_files.sort do |a, b|
    if slow_features.include?(a)
      1
    elsif slow_features.include?(b)
      -1
    else
      a <=> b
    end
  end

  # Get the singleton class/eigenclass for configuration
  klass = class << configuration; self; end

  klass.send(:undef_method, :feature_files)
  klass.send(:define_method, :feature_files) { features_files }
end

Aruba.configure do |config|
  config.exit_timeout = 60
  config.remove_ansi_escape_sequences = false
end

Before do
  announcer.activate(:stdout)
  announcer.activate(:stderr)
end

After do |s|
  # Tell Cucumber to quit after this scenario is done - if it failed.
  Cucumber.wants_to_quit = true if s.failed? and ENV["FAIL_FAST"] == 'Y'
end

After('@auth') do
  @cluster.disable_authentication
end

After('@ssl') do
  @cluster.disable_ssl
end

After('@netblock') do
  @cluster.unblock_nodes
end

After('@client_failures') do
  @cluster.restart
end

##############################################################################################################
# The following matchers and aruba SpawnProcess monkey-patch cause aruba to write command output to file
#     with binmode to avoid encoding / translation issues. The matchers take the "binary" output and re-interpret
#     as utf8. This works around (resolves?) a behavior difference in jruby9k where cukes were failing when output
#     included non-ascii characters. RUBY-167
##############################################################################################################

RSpec::Matchers.define :include_output_string do |expected|
  match do |actual|
    actual.force_encoding("UTF-8")
    @expected = Regexp.new(Regexp.escape(sanitize_text(expected.to_s)), Regexp::MULTILINE)
    @actual   = sanitize_text(actual)

    values_match? @expected, @actual
  end

  diffable

  description { "string includes: #{description_of expected}" }
end

RSpec::Matchers.define :match_output_string do |expected|
  match do |actual|
    actual.force_encoding("UTF-8")
    @expected = Regexp.new(unescape_text(expected), Regexp::MULTILINE)
    @actual   = sanitize_text(actual)

    values_match? @expected, @actual
  end

  diffable

  description { "output string matches: #{description_of expected}" }
end

RSpec::Matchers.define :output_string_eq do |expected|
  match do |actual|
    actual.force_encoding("UTF-8")
    @expected = sanitize_text(expected.to_s)
    @actual   = sanitize_text(actual.to_s)

    values_match? @expected, @actual
  end

  diffable

  description { "output string is eq: #{description_of expected}" }
end

module Aruba
  module Processes
    class SpawnProcess
      def start
        # rubocop:disable Metrics/LineLength
        fail CommandAlreadyStartedError, %(Command "#{commandline}" has already been started. Please `#stop` the command first and `#start` it again. Alternatively use `#restart`.\n#{caller.join("\n")}) if started?
        # rubocop:enable Metrics/LineLength

        @started = true

        @process = ChildProcess.build(*[command_string.to_a, arguments].flatten)
        @stdout_file = Tempfile.new('aruba-stdout-')
        @stderr_file = Tempfile.new('aruba-stderr-')


        @stdout_file.sync = true
        @stderr_file.sync = true

        @stdout_file.binmode
        @stderr_file.binmode

        @exit_status = nil
        @duplex = true

        before_run

        @process.leader = true
        @process.io.stdout = @stdout_file
        @process.io.stderr = @stderr_file
        @process.duplex = @duplex
        @process.cwd = @working_directory

        @process.environment.update(environment)

        begin
          Aruba.platform.with_environment(environment) do
            @process.start
            sleep startup_wait_time
          end
        rescue ChildProcess::LaunchError => e
          raise LaunchError, "It tried to start #{cmd}. " + e.message
        end

        after_run

        yield self if block_given?
      end
    end
  end
end

