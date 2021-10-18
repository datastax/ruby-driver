# encoding: utf-8

#--
# Copyright DataStax, Inc.
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
        describe(Simple) do
          subject { Simple.new }

          describe('#replication_map') do
            let(:hosts) {
              [
                Host.new('127.0.0.1',  111, 'rack1', 'dc1'),
                Host.new('127.0.0.2',  112, 'rack1', 'dc1'),
                Host.new('127.0.0.3',  113, 'rack1', 'dc1'),
              ]
            }

            let(:token_hosts) {
              {
                -9000000000000000000 => hosts[0],
                -8000000000000000000 => hosts[0],
                -7000000000000000000 => hosts[1],
                -6000000000000000000 => hosts[1],
                -5000000000000000000 => hosts[2],
                -4000000000000000000 => hosts[2],
                -3000000000000000000 => hosts[0],
                -2000000000000000000 => hosts[0],
                -1000000000000000000 => hosts[1],
                +0000000000000000000 => hosts[1],
                +1000000000000000000 => hosts[2],
                +2000000000000000000 => hosts[2],
              }
            }

            let(:token_ring) {
              token_hosts.keys
            }

            let(:replication_options) {
              {
                'replication_factor' => 3,
              }
            }

            it do
              replication_map = subject.replication_map(token_hosts, token_ring, replication_options)
              expect(replication_map[token_ring[0]].map(&:ip)).to eq([
                '127.0.0.1',
                '127.0.0.2',
                '127.0.0.3',
              ])
              expect(replication_map[token_ring[2]].map(&:ip)).to eq([
                '127.0.0.2',
                '127.0.0.3',
                '127.0.0.1',
              ])
            end
          end
        end
      end
    end
  end
end
