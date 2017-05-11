# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
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
#++

ENV['CASSANDRA_HOST'] ||= '127.0.0.1'

require 'bundler/setup'

require File.dirname(__FILE__) + '/../support/retry.rb'
require File.dirname(__FILE__) + '/../support/ccm.rb'

require 'rspec/wait'
require 'rspec/collection_matchers'

require 'support/bytes_helper'
require 'support/await_helper'
require 'support/fake_io_reactor'
require 'support/fake_cluster_registry'
require 'support/stub_io_reactor'

RSpec.configure do |config|
  # suppress ruby warnings
  config.warnings = false

  config.expect_with(:rspec) do |c|
    c.syntax = [:should, :expect]
  end
  config.mock_with(:rspec) do |c|
    c.syntax = [:should, :expect]
  end

  config.before(:context, :integration) do
    CCM.setup_cluster(1, 1)
  end
end

unless ENV['COVERAGE'] == 'no' || RUBY_ENGINE == 'rbx'
  require 'simplecov'

  SimpleCov.start do
    command_name 'RSpec'
  end
end

require 'cassandra'
require 'cassandra/compression/compressors/snappy'
require 'cassandra/compression/compressors/lz4'
