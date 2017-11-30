source 'https://rubygems.org/'

gemspec

gem 'cliver',        group: [:development, :test]
gem 'lz4-ruby',      group: [:development, :test]
gem 'rake-compiler', group: [:development, :test]
gem 'snappy',        group: [:development, :test]

group :development do
  platforms :mri_19 do
    gem 'perftools.rb'
  end
  gem 'rubocop', '~> 0.49', require: false
end

group :test do
  gem 'ansi'
  gem 'aruba'
  gem 'cucumber'
  gem 'delorean'
  gem 'minitest', '< 5.0.0'
  gem 'os'
  gem 'rspec'
  gem 'rspec-collection_matchers'
  gem 'rspec-wait'
  gem 'simplecov'
end

group :docs do
  gem 'yard'
end
