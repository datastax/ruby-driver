# encoding: utf-8

require 'bundler/setup'

require 'rspec/core/rake_task'
require 'cucumber/rake/task'

ENV["FAIL_FAST"] ||= 'Y'

desc 'Tag & release the gem'
task :release => :test do
  $: << 'lib'
  require 'cql/version'

  project_name = 'cassandra-driver'
  version_string = "v#{Cql::VERSION}"
  
  unless %x(git tag -l).split("\n").include?(version_string)
    system %(git tag -a #{version_string} -m #{version_string})
  end

  system %(git push && git push --tags; gem build #{project_name}.gemspec && gem push #{project_name}-*.gem && mv #{project_name}-*.gem pkg)
end


task :start_rspec_cluster do
    `ccm create 1-node-cluster --nodes 1 --start --ipprefix 127.0.0. --binary-protocol --cassandra-version 2.0.9`
end

task :delete_rspec_cluster do
    `ccm remove 1-node-cluster`
end


RSpec::Core::RakeTask.new(:rspec => :start_rspec_cluster) do |t|
    t.rspec_opts = "--fail-fast" if ENV["FAIL_FAST"] == 'Y'
end

Rake::Task[:rspec].enhance do
  Rake::Task[:delete_rspec_cluster].invoke
end

Cucumber::Rake::Task.new(:cucumber) do |t|
    t.cucumber_opts = '--tags ~@todo'
end

desc 'Run all tests'
task :test => [:rspec, :cucumber]

desc 'Generate documentation'
task :docs do
  require 'nanoc'
  load 'nanoc/setup.rb'

  Nanoc::Site.new('.').compile
end
