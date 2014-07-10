source 'https://rubygems.org/'

gemspec

gem 'aruba'
gem 'rake'
gem 'snappy'
gem 'lz4-ruby'

group :development do
  gem 'pry'
  platforms :mri do
    gem 'yard'
    gem 'redcarpet'
  end
  platforms :mri_19 do
    gem 'perftools.rb'
  end
end

group :test do
  gem 'rspec'
  gem 'simplecov'
  gem 'coveralls'
  gem 'cucumber'
end