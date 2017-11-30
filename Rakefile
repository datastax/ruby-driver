# encoding: utf-8

require 'bundler/setup'

require 'rspec/core/rake_task'
require 'cucumber/rake/task'
require 'rake/testtask'
require 'bundler/gem_tasks'

ENV['FAIL_FAST'] ||= 'Y'

RSpec::Core::RakeTask.new(rspec: :compile)

# We separate interactive from non-interactive features because jruby 9k sometimes has trouble
# closing its pipe to the child process in interactive features.

Cucumber::Rake::Task.new({ cucumber_interactive: :compile }, 'Run cucumber features that are interactive') do |t|
  t.profile = 'interactive'
end

Cucumber::Rake::Task.new({ cucumber_noninteractive: :compile }, 'Run cucumber features that are non-interactive') do |t|
  t.profile = 'non_interactive'
end

desc 'Run cucumber features'
task cucumber: [:cucumber_noninteractive, :cucumber_interactive]

desc 'Run all tests'
task test: [:rspec, :integration, :cucumber]

ruby_engine = defined?(RUBY_ENGINE) ? RUBY_ENGINE : 'ruby'

case ruby_engine
when 'jruby'
  require 'rake/javaextensiontask'

  Rake::JavaExtensionTask.new('cassandra_murmur3')
else
  require 'rake/extensiontask'

  Rake::ExtensionTask.new('cassandra_murmur3')
end

Rake::TestTask.new(integration: :compile) do |t|
  t.libs.push 'lib'
  t.test_files = FileList['integration/*_test.rb',
                          'integration/security/*_test.rb',
                          'integration/load_balancing/*_test.rb',
                          'integration/types/*_test.rb',
                          'integration/functions/*_test.rb',
                          'integration/indexes/*_test.rb']
  t.verbose = true
end

Rake::TestTask.new(stress: :compile) do |t|
  t.libs.push 'lib'
  t.test_files = FileList['integration/stress_tests/*_test.rb']
  t.verbose = true
end
