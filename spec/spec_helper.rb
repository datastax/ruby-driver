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

require 'bundler/setup'

require 'rspec/collection_matchers'

require 'support/bytes_helper'
require 'support/await_helper'
require 'support/fake_io_reactor'
require 'support/fake_cluster_registry'

unless ENV['COVERAGE'] == 'no' || RUBY_ENGINE == 'rbx'
  require 'coveralls'
  require 'simplecov'
end

require 'cql'
require 'cql/compression/snappy_compressor'
require 'cql/compression/lz4_compressor'

RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
  config.mock_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
end
