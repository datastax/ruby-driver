source 'https://rubygems.org/'

gemspec

gem 'snappy',        group: [:development, :test]
gem 'lz4-ruby',      group: [:development, :test]
gem 'rake-compiler', group: [:development, :test]
gem 'cliver',        group: [:development, :test]

group :development do
  platforms :mri_19 do
    gem 'perftools.rb'
  end
  gem 'rubocop', '~> 0.36', require: false
end

group :test do
  gem 'rspec'
  gem 'rspec-wait'
  gem 'rspec-collection_matchers'
  gem 'simplecov'
  gem 'cucumber'
  gem 'aruba'
  gem 'os'
  gem 'minitest', '< 5.0.0'
  gem 'ansi'
  gem 'delorean'
end

group :docs do
  gem 'yard'
end
