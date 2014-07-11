# encoding: utf-8

require 'bundler/setup'

require 'rspec/core/rake_task'
require 'cucumber/rake/task'

RSpec::Core::RakeTask.new(:spec) do |t|
   t.rspec_opts = "--fail-fast"
end
Cucumber::Rake::Task.new(:features) do |t|
  t.cucumber_opts = '--tags ~@todo'
end

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

desc 'Run all tests'
task :test => [:spec, :features]
