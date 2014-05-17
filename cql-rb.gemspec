# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'cql/version'


Gem::Specification.new do |s|
  s.name          = 'cql-rb'
  s.version       = Cql::VERSION.dup
  s.authors       = ['Theo Hultberg']
  s.email         = ['theo@iconara.net']
  s.homepage      = 'http://github.com/iconara/cql-rb'
  s.summary       = %q{Cassandra CQL3 driver}
  s.description   = %q{A pure Ruby CQL3 driver for Cassandra}
  s.license       = 'Apache License 2.0'

  s.files         = Dir['lib/**/*.rb', 'bin/*', 'README.md', '.yardopts']
  s.test_files    = Dir['spec/**/*.rb']
  s.require_paths = %w(lib)
  s.bindir        = 'bin'

  s.platform = Gem::Platform::RUBY
  s.required_ruby_version = '>= 1.9.3'

  s.add_dependency 'ione', '~> 1'
end
