# encoding: utf-8

require 'bundler/setup'

require 'rspec/core/rake_task'
require 'cucumber/rake/task'

ENV["FAIL_FAST"] ||= 'Y'

desc 'Tag & release the gem'
task :release => :test do
  $: << 'lib'
  require 'cassandra/version'

  project_name = 'cassandra-driver'
  version_string = "v#{Cassandra::VERSION}"
  
  unless %x(git tag -l).split("\n").include?(version_string)
    system %(git tag -a #{version_string} -m #{version_string})
  end

  system %(git push && git push --tags; gem build #{project_name}.gemspec && gem push #{project_name}-*.gem && mv #{project_name}-*.gem pkg)
end

RSpec::Core::RakeTask.new(:rspec)

Cucumber::Rake::Task.new(:cucumber)

desc 'Run all tests'
task :test => [:rspec, :cucumber]

desc 'Generate documentation'
task :docs do
  require 'nanoc'
  load 'nanoc/setup.rb'

  Nanoc::Site.new('.').compile
end
