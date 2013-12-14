source 'https://rubygems.org/'

gemspec

gem 'rake'

group :development do
  gem 'pry'
  gem 'viiite'
  gem 'travis-lint'
  gem 'snappy'
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
end