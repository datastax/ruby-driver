# encoding: utf-8

require 'bundler/setup'

require 'rspec/core/rake_task'
require 'cucumber/rake/task'

ENV["FAIL_FAST"] ||= 'Y'


desc 'Tag & release the gem'
task :release => :test do
  $: << 'lib'
  require 'cql/version'

  project_name = 'cql-rb'
  version_string = "v#{Cql::VERSION}"
  
  unless %x(git tag -l).split("\n").include?(version_string)
    system %(git tag -a #{version_string} -m #{version_string})
  end

  system %(git push && git push --tags; gem build #{project_name}.gemspec && gem push #{project_name}-*.gem && mv #{project_name}-*.gem pkg)
end


desc 'Create and start a 1-node Cassandra cluster for RSpec tests'
task :start_rspec_cluster do
    `ccm create 1-node-cluster --nodes 1 --start --ipprefix 127.0.0. --binary-protocol --cassandra-version 2.0.9`
end


desc 'Delete the 1-node Cassandra cluster created for RSpec tests'
task :delete_rspec_cluster do
    `ccm remove 1-node-cluster`
end


desc 'Run all RSpec tests, failing fast'
task :rspec do
    Rake::Task["start_rspec_cluster"].execute
    RSpec::Core::RakeTask.new(:rspec_tests) do |t|
        t.rspec_opts = "--fail-fast" if ENV["FAIL_FAST"] == 'Y'
    end
    Rake::Task["rspec_tests"].execute
    Rake::Task["delete_rspec_cluster"].execute
end


desc 'Execute all Cucumber tests that are not annotated with @todo'
task :cucumber do
    Cucumber::Rake::Task.new(:cucumber_tests) do |t|
        t.cucumber_opts = '--tags ~@todo'
    end
    Rake::Task["cucumber_tests"].execute
end


desc 'Run all tests'
task :test => [:rspec, :cucumber]
