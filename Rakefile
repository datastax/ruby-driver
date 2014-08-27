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

RSpec::Core::RakeTask.new(:rspec) do |t|
  t.rspec_opts = "--fail-fast" if ENV["FAIL_FAST"] == 'Y'
end

Cucumber::Rake::Task.new(:cucumber) do |t|
  cassandra_version = ENV['CASSANDRA_VERSION'] || '2.0.9'
  cassandra_version_tags = ''

  if cassandra_version.start_with?('2.0.')
    cassandra_version_tags += ',@cassandra-version-2.0'

    if cassandra_version.sub('2.0.', '').to_i >= 9
      cassandra_version_tags += ',@cassandra-version-2.0.9+'
    end
  end

  if cassandra_version.start_with?('1.2')
    cassandra_version_tags = ',@cassandra-version-1.2'
  end

  t.cucumber_opts = ('--tags ~@todo --tags ~@cassandra-version-specific' + cassandra_version_tags)
end

desc 'Run all tests'
task :test => [:rspec, :cucumber]

desc 'Generate documentation'
task :docs do
  require 'nanoc'
  load 'nanoc/setup.rb'

  Nanoc::Site.new('.').compile
end
