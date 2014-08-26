# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'cassandra/version'

Gem::Specification.new do |s|
  s.name          = 'cassandra-driver'
  s.version       = Cassandra::VERSION.dup
  s.authors       = ['Theo Hultberg', 'Bulat Shakirzyanov']
  s.email         = ['theo@iconara.net', 'bulat.shakirzyanov@datastax.com']
  s.homepage      = 'http://riptano.github.io/ruby-driver'
  s.summary       = %q{Cassandra driver}
  s.description   = %q{A pure Ruby driver for Cassandra}
  s.license       = 'Apache License 2.0'

  s.files         = Dir['lib/**/*.rb', 'bin/*', 'README.md', '.yardopts']
  s.test_files    = Dir['spec/**/*.rb']
  s.require_paths = %w(lib)
  s.bindir        = 'bin'

  s.platform = Gem::Platform::RUBY
  s.required_ruby_version = '>= 1.9.3'

  s.add_dependency 'ione', '~> 1.0'
end
