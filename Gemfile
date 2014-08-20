source 'https://rubygems.org/'

gemspec

gem 'rake', '= 10.3.2'
gem 'snappy', '= 0.0.10'
gem 'lz4-ruby', '= 0.3.3'

group :development do
  platforms :mri_19 do
    gem 'perftools.rb', '= 2.0.1'
  end
end

group :test do
  gem 'rspec', '= 3.0.0'
  gem 'rspec-collection_matchers', '= 1.0.0'
  gem 'simplecov', '= 0.9.0'
  gem 'coveralls', '= 0.7.1'
  gem 'cucumber', '= 1.3.16'
  gem 'aruba', '= 0.6.0'
end

group :docs do
  gem 'gherkin', '= 2.12.2'
  gem 'yard', '= 0.8.7.4'
  gem 'htmlbeautifier', '= 0.0.9'

  platforms :mri_19 do
    gem 'nanoc', '= 3.7.2'
    gem 'nanoc-toolbox', '= 0.2.0'
    gem 'compass', '= 1.0.1'
    gem 'bootstrap-sass', '= 3.2.0.1'
    gem 'nokogiri', '= 1.6.3.1'
    gem 'rubypants', '= 0.2.0'
    gem 'guard', '= 2.6.1'
    gem 'guard-nanoc', '= 1.0.2'
    gem 'pygments.rb', '= 0.6.0'
    gem 'redcarpet', '= 3.1.2'
  end
end
