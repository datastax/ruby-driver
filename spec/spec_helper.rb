# encoding: utf-8

# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ENV['CASSANDRA_HOST'] ||= 'localhost'

require File.dirname(__FILE__) + '/../support/ccm.rb'

require 'bundler/setup'

require 'rspec/collection_matchers'

require 'support/bytes_helper'
require 'support/await_helper'
require 'support/fake_io_reactor'
require 'support/fake_cluster_registry'

RSpec.configure do |config|
  config.expect_with(:rspec) do |c|
    c.syntax = [:should, :expect]
  end
  config.mock_with(:rspec) do |c|
    c.syntax = [:should, :expect]
  end

  config.include CCM, :integration

  config.before(:context, :integration) do
    setup_cluster(1, 1)
  end
end

unless ENV['COVERAGE'] == 'no' || RUBY_ENGINE == 'rbx'
  require 'coveralls'
  require 'simplecov'
end

require 'cassandra'
require 'cassandra/compression/snappy_compressor'
require 'cassandra/compression/lz4_compressor'
