# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe ConnectionManager do
      let :manager do
        described_class.new
      end

      let :connections do
        [double(:connection1), double(:connection2), double(:connection3)]
      end

      before do
        connections.each do |c|
          c.stub(:on_closed) do |&listener|
            c.stub(:closed_listener).and_return(listener)
          end
        end
      end

      describe '#add_connections' do
        it 'registers as a close listener on each connection' do
          manager.add_connections(connections)
          connections.each { |c| c.should have_received(:on_closed) }
        end

        it 'stops managing the connection when the connection closes' do
          manager.add_connections(connections)
          connections.each { |c| c.closed_listener.call }
          expect { manager.random_connection }.to raise_error(NotConnectedError)
        end
      end

      describe '#connected?' do
        it 'returns true when there are connections' do
          manager.add_connections(connections)
          manager.should be_connected
        end

        it 'returns false when there are no' do
          manager.should_not be_connected
        end
      end

      describe '#snapshot' do
        it 'returns a copy of the list of connections' do
          manager.add_connections(connections)
          s = manager.snapshot
          s.should == connections
          s.should_not equal(connections)
        end
      end

      describe '#random_connection' do
        before do
          connections.each { |c| c.stub(:on_closed) }
        end

        it 'returns one of the connections it is managing' do
          manager.add_connections(connections)
          connections.should include(manager.random_connection)
        end

        it 'raises a NotConnectedError when there are no connections' do
          expect { manager.random_connection }.to raise_error(NotConnectedError)
        end
      end

      describe '#select_connections' do
        it 'returns a filtered list of connections' do
          connections[0].stub(:keyspace).and_return('first')
          connections[1].stub(:keyspace).and_return('second')
          connections[2].stub(:keyspace).and_return('first')
          manager.add_connections(connections)
          filtered = manager.select_connections { |c| c.keyspace != 'first' }
          filtered.should == [connections[1]]
        end
      end
    end
  end
end