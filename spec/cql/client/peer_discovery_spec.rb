# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe PeerDiscovery do
      let :peer_discovery do
        described_class.new(seed_connections)
      end

      describe '#new_hosts' do
        let :seed_connections do
          [
            FakeConnection.new('host0', 9042, 5, {:data_center => 'dc0', :host_id => Uuid.new('00000000-0000-0000-0000-000000000000')}),
            FakeConnection.new('host1', 9042, 5, {:data_center => 'dc0', :host_id => Uuid.new('11111111-1111-1111-1111-111111111111')}),
            FakeConnection.new('host2', 9042, 5, {:data_center => 'dc0', :host_id => Uuid.new('22222222-2222-2222-2222-222222222222')}),
          ]
        end

        let :peer_metadata do
          [
            ['system', 'peers', 'peer', :inet],
            ['system', 'peers', 'data_center', :varchar],
            ['system', 'peers', 'host_id', :uuid],
            ['system', 'peers', 'rpc_address', :inet],
          ]
        end

        let :peer_rows do
          [
            {'peer' => IPAddr.new('2.0.0.0'), 'rpc_address' => IPAddr.new('1.0.0.0'), 'data_center' => 'dc0', 'host_id' => Uuid.new('00000000-0000-0000-0000-000000000000')},
            {'peer' => IPAddr.new('2.0.0.1'), 'rpc_address' => IPAddr.new('1.0.0.1'), 'data_center' => 'dc0', 'host_id' => Uuid.new('11111111-1111-1111-1111-111111111111')},
            {'peer' => IPAddr.new('2.0.0.2'), 'rpc_address' => IPAddr.new('1.0.0.2'), 'data_center' => 'dc0', 'host_id' => Uuid.new('22222222-2222-2222-2222-222222222222')},
            {'peer' => IPAddr.new('2.0.0.3'), 'rpc_address' => IPAddr.new('1.0.0.3'), 'data_center' => 'dc0', 'host_id' => Uuid.new('33333333-3333-3333-3333-333333333333')},
            {'peer' => IPAddr.new('2.0.0.4'), 'rpc_address' => IPAddr.new('1.0.0.4'), 'data_center' => 'dc0', 'host_id' => Uuid.new('44444444-4444-4444-4444-444444444444')},
          ]
        end

        let :peer_rows_response do
          Protocol::RowsResultResponse.new(peer_rows, peer_metadata, nil, nil)
        end

        before do
          seed_connections.each do |connection|
            connection.handle_request do |request|
              peer_rows_response
            end
          end
        end

        it 'selects all rows from the "peers" system table' do
          peer_discovery.new_hosts.value
          selected_connection = seed_connections.find { |c| c.requests.any? }
          request = selected_connection.requests.first
          columns, table = request.cql.scan(/SELECT (.*) FROM (\S+)/).flatten
          table.should == 'system.peers'
          columns.should include('peer')
          columns.should include('data_center')
          columns.should include('host_id')
          columns.should include('rpc_address')
        end

        it 'returns a future that resolves to the addresses of all peers that were not already in the set of seed nodes' do
          new_hosts = peer_discovery.new_hosts
          new_hosts.value.should == %w[1.0.0.3 1.0.0.4]
        end

        it 'returns an empty list of addresses when there are no peers that are not in the set of seed nodes' do
          peer_rows.pop(2)
          new_hosts = peer_discovery.new_hosts
          new_hosts.value.should be_empty
        end

        it 'returns the value of the "peer" column when the "rpc_address" column contains "0.0.0.0"' do
          peer_rows.each do |row|
            row['rpc_address'] = IPAddr.new('0.0.0.0')
          end
          new_hosts = peer_discovery.new_hosts
          new_hosts.value.should == %w[2.0.0.3 2.0.0.4]
        end

        it 'only returns addresses to nodes that are in the same data centers as the seed nodes' do
          peer_rows[3]['data_center'] = 'dc1'
          new_hosts = peer_discovery.new_hosts
          new_hosts.value.should == %w[1.0.0.4]
        end
      end
    end
  end
end