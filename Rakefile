# encoding: utf-8

require 'bundler/setup'

require 'rspec/core/rake_task'
require 'cucumber/rake/task'

ENV["FAIL_FAST"] ||= 'Y'

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
