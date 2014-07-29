source 'https://rubygems.org/'

gemspec

gem 'rake'
gem 'snappy'
gem 'lz4-ruby'

group :development do
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
  gem 'aruba'
end

group :docs do
  gem 'nanoc'
  gem 'gherkin'
  # gem 'redcarpet'
  gem 'pygments.rb'
  gem 'compass'
  gem 'bootstrap-sass'
  gem 'nokogiri'
  gem 'guard'
  gem 'guard-nanoc'
end
