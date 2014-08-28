source 'https://rubygems.org/'

gemspec

gem 'rake'
gem 'snappy'
gem 'lz4-ruby'

group :development do
  platforms :mri_19 do
    gem 'perftools.rb'
  end
end

group :test do
  gem 'rspec'
  gem 'rspec-collection_matchers'
  gem 'simplecov'
  gem 'coveralls'
  gem 'cucumber'
  gem 'aruba'
end

group :docs do
  gem 'gherkin'
  gem 'yard'
  gem 'htmlbeautifier'

  platforms :mri_19 do
    gem 'nanoc'
    gem 'nanoc-toolbox'
    gem 'compass'
    gem 'bootstrap-sass'
    gem 'nokogiri'
    gem 'rubypants'
    gem 'guard'
    gem 'guard-nanoc'
    gem 'pygments.rb'
    gem 'redcarpet'
    gem 'ditaarb'
  end
end
