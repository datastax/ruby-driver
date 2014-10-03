# encoding: utf-8

require 'bundler/setup'

require 'rspec/core/rake_task'
require 'cucumber/rake/task'
require 'rake/testtask'
require 'bundler/gem_tasks'

ENV["FAIL_FAST"] ||= 'Y'

RSpec::Core::RakeTask.new(:rspec => :compile)

Cucumber::Rake::Task.new(:cucumber => :compile)

desc 'Run all tests'
task :test => [:rspec, :integration, :cucumber]

desc 'Generate documentation'
task :docs do
  require 'nanoc'
  load 'nanoc/setup.rb'

  Nanoc::Site.new('.').compile
end

ruby_engine = defined?(RUBY_ENGINE)? RUBY_ENGINE : 'ruby'

case ruby_engine
when 'jruby'
  require 'rake/javaextensiontask'

  Rake::JavaExtensionTask.new('cassandra_murmur3')
  Rake::JavaExtensionTask.new('cql_scanner')
  Rake::Task['compile'].prerequisites.unshift(FileList['ext/cql_scanner/CqlScannerService.java'])
else
  require 'rake/extensiontask'

  Rake::ExtensionTask.new('cassandra_murmur3')
  Rake::ExtensionTask.new('cql_scanner')
  Rake::Task['compile'].prerequisites.unshift(FileList['ext/cql_scanner/cql_scanner.c'])
end

Rake::TestTask.new(:integration => :compile) do |t|
  t.libs.push "lib"
  t.test_files = FileList['integration/*_test.rb',
                  'integration/security/*_test.rb',
                  'integration/load_balancing/*_test.rb']
  t.verbose = true
end

task :check_ragel do
  require 'cliver'
  Cliver.detect!('ragel')
end

file 'ext/cql_scanner/cql_scanner.c' => [:check_ragel] do
  system('ragel', '-C', '-o', 'ext/cql_scanner/cql_scanner.c', 'ragel/scanner_c.rl')
end

CLOBBER.include('ext/cql_scanner/cql_scanner.c')

file 'ext/cql_scanner/CqlScannerService.java' => [:check_ragel] do
  system('ragel', '-J', '-o', 'ext/cql_scanner/CqlScannerService.java', 'ragel/scanner_java.rl')
end

CLOBBER.include('ext/cql_scanner/CqlScannerService.java')
