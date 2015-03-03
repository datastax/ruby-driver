# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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
        describe(NetworkTopology) do
          subject { NetworkTopology.new }

          describe('#replication_map') do
            let(:hosts) {
              [
                Host.new('127.0.0.1',  111, 'rack1', 'dc1'),
                Host.new('127.0.0.5',  211, 'rack1', 'dc2'),
                Host.new('127.0.0.9',  311, 'rack1', 'dc3'),
                Host.new('127.0.0.2',  112, 'rack1', 'dc1'),
                Host.new('127.0.0.6',  212, 'rack1', 'dc2'),
                Host.new('127.0.0.10', 312, 'rack1', 'dc3'),
                Host.new('127.0.0.3',  121, 'rack2', 'dc1'),
                Host.new('127.0.0.7',  221, 'rack2', 'dc2'),
                Host.new('127.0.0.11', 321, 'rack2', 'dc3'),
                Host.new('127.0.0.12', 322, 'rack2', 'dc3'),
                Host.new('127.0.0.4',  122, 'rack2', 'dc1'),
                Host.new('127.0.0.8',  222, 'rack2', 'dc2'),
              ]
            }

            let(:token_ring) {
              [
                -9000000000000000000,
                -8000000000000000000,
                -7000000000000000000,
                -6000000000000000000,
                -5000000000000000000,
                -4000000000000000000,
                -3000000000000000000,
                -2000000000000000000,
                -1000000000000000000,
                +0000000000000000000,
                +1000000000000000000,
                +2000000000000000000
              ]
            }

            let(:token_hosts) {
              result = ::Hash.new
              token_ring.zip(hosts) do |token, host|
                result[token] = host
              end
              result
            }

            context('with equal distribution') do
              let(:replication_options) {
                {
                  'dc1' => '2',
                  'dc2' => '2',
                  'dc3' => '2',
                }
              }

              it 'maps to hosts from different datacenters and racks' do
                replication_map = subject.replication_map(token_hosts, token_ring, replication_options)
                expect(replication_map[token_ring[0]].sort_by(&:ip).map do |host|
                  [host.ip, host.datacenter, host.rack]
                end).to eq([
                  ['127.0.0.1',  'dc1', 'rack1'],
                  ['127.0.0.11', 'dc3', 'rack2'],
                  ['127.0.0.3',  'dc1', 'rack2'],
                  ['127.0.0.5',  'dc2', 'rack1'],
                  ['127.0.0.7',  'dc2', 'rack2'],
                  ['127.0.0.9',  'dc3', 'rack1'],
                ])
              end
            end

            context('with datacenters missing replication factor') do
              let(:replication_options) {
                {
                  'dc1' => '2',
                  'dc2' => '2'
                }
              }

              it 'skips those datacenters in replication map' do
                replication_map = subject.replication_map(token_hosts, token_ring, replication_options)
                expect(replication_map[token_ring[0]].sort_by(&:ip).map do |host|
                  [host.ip, host.datacenter, host.rack]
                end).to eq([
                  ['127.0.0.1', 'dc1', 'rack1'],
                  ['127.0.0.3', 'dc1', 'rack2'],
                  ['127.0.0.5', 'dc2', 'rack1'],
                  ['127.0.0.7', 'dc2', 'rack2']
                ])
              end
            end
          end
        end
      end
    end
  end
end
