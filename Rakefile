# encoding: utf-8

require 'rspec/core/rake_task'


RSpec::Core::RakeTask.new(:spec)

desc 'Tag & release the gem'
task :release => :spec do
  $: << 'lib'
  require 'cql/version'

  project_name = 'cql-rb'
  version_string = "v#{Cql::VERSION}"
  
  unless %x(git tag -l).split("\n").include?(version_string)
    system %(git tag -a #{version_string} -m #{version_string})
  end

  system %(git push && git push --tags; gem build #{project_name}.gemspec && gem push #{project_name}-*.gem && mv #{project_name}-*.gem pkg)
end
