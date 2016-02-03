# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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

require 'spec_helper'

module Cassandra
  class Cluster
    class Schema
      module ReplicationStrategies
        describe(None) do
          subject { None.new }

          describe('#replication_map') do
            it 'maps tokens to primary replicas only' do
              token_hosts = {
                '123' => '127.0.0.1',
                'qwe' => '127.0.0.2',
                'asd' => '127.0.0.3',
              }
              token_ring = []
              replication_options = {}

              expect(subject.replication_map(token_hosts, token_ring, replication_options)).to eq({
                '123' => ['127.0.0.1'],
                'qwe' => ['127.0.0.2'],
                'asd' => ['127.0.0.3']
              })
            end
          end
        end
      end
    end
  end
end
