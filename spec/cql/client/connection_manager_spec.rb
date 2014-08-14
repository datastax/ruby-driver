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
          expect { manager.random_connection }.to raise_error(Errors::NotConnectedError)
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

        it 'raises a Errors::NotConnectedError when there are no connections' do
          expect { manager.random_connection }.to raise_error(Errors::NotConnectedError)
        end
      end

      describe '#each_connection' do
        it 'yields each connection to the given block' do
          manager.add_connections(connections)
          yielded = []
          manager.each_connection { |c| yielded << c }
          yielded.should == connections
        end

        it 'is aliased as #each' do
          manager.add_connections(connections)
          yielded = []
          manager.each { |c| yielded << c }
          yielded.should == connections
        end

        it 'returns an Enumerable when no block is given' do
          manager.each.should be_an(Enumerable)
        end

        it 'raises a Errors::NotConnectedError when there are no connections' do
          expect { manager.each_connection { } }.to raise_error(Errors::NotConnectedError)
        end
      end

      context 'as an Enumerable' do
        before do
          connections.each_with_index { |c, i| c.stub(:index).and_return(i) }
        end

        it 'can be mapped' do
          manager.add_connections(connections)
          manager.map { |c| c.index }.should == [0, 1, 2]
        end

        it 'can be filtered' do
          manager.add_connections(connections)
          manager.select { |c| c.index % 2 == 0 }.should == [connections[0], connections[2]]
        end

        it 'raises a Errors::NotConnectedError when there are no connections' do
          expect { manager.select { } }.to raise_error(Errors::NotConnectedError)
        end
      end
    end
  end
end