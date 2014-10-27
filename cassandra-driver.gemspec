# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'cassandra/version'

Gem::Specification.new do |s|
  s.name          = 'cassandra-driver'
  s.version       = Cassandra::VERSION.dup
  s.authors       = ['Theo Hultberg', 'Bulat Shakirzyanov']
  s.email         = ['theo@iconara.net', 'bulat.shakirzyanov@datastax.com']
  s.homepage      = 'http://datastax.github.io/ruby-driver'
  s.summary       = %q{Datastax Ruby Driver for Apache Cassandra}
  s.description   = %q{A pure Ruby driver for Apache Cassandra}
  s.license       = 'Apache License 2.0'
  s.files         = Dir['lib/**/*.rb', 'README.md', '.yardopts']
  s.require_paths = %w(lib)

  s.extra_rdoc_files = ['README.md']
  s.rdoc_options << '--title' << 'Datastax Ruby Driver' << '--main' << 'README.md' << '--line-numbers'

  s.required_ruby_version = '>= 1.9.3'

  if defined?(JRUBY_VERSION)
    s.platform = 'java'
    s.files << 'lib/cassandra_murmur3.jar'
  else
    s.platform = Gem::Platform::RUBY
    s.extensions = 'ext/cassandra_murmur3/extconf.rb'
    s.files << 'ext/cassandra_murmur3/cassandra_murmur3.c'
  end

  s.add_runtime_dependency 'ione', '~> 1.2.0.pre8'

  s.add_development_dependency 'bundler', '~> 1.6'
  s.add_development_dependency 'rake', '~> 10.0'
end
