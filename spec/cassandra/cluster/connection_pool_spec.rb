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
    describe ConnectionPool do
      let :pool do
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
          pool.add_connections(connections)
          connections.each { |c| c.should have_received(:on_closed) }
        end

        it 'stops managing the connection when the connection closes' do
          pool.add_connections(connections)
          connections.each { |c| c.closed_listener.call }
          expect { pool.random_connection }.to raise_error(Errors::IOError)
        end
      end

      describe '#connected?' do
        it 'returns true when there are connections' do
          pool.add_connections(connections)
          pool.should be_connected
        end

        it 'returns false when there are no' do
          pool.should_not be_connected
        end
      end

      describe '#snapshot' do
        it 'returns a copy of the list of connections' do
          pool.add_connections(connections)
          s = pool.snapshot
          s.should == connections
          s.should_not equal(connections)
        end
      end

      describe '#random_connection' do
        before do
          connections.each { |c| c.stub(:on_closed) }
        end

        it 'returns one of the connections it is managing' do
          pool.add_connections(connections)
          connections.should include(pool.random_connection)
        end

        it 'raises a Errors::IOError when there are no connections' do
          expect { pool.random_connection }.to raise_error(Errors::IOError)
        end
      end

      describe '#each_connection' do
        it 'yields each connection to the given block' do
          pool.add_connections(connections)
          yielded = []
          pool.each_connection { |c| yielded << c }
          yielded.should == connections
        end

        it 'is aliased as #each' do
          pool.add_connections(connections)
          yielded = []
          pool.each { |c| yielded << c }
          yielded.should == connections
        end

        it 'returns an Enumerable when no block is given' do
          pool.each.should be_an(Enumerable)
        end

        it 'raises a Errors::IOError when there are no connections' do
          expect { pool.each_connection { } }.to raise_error(Errors::IOError)
        end
      end

      context 'as an Enumerable' do
        before do
          connections.each_with_index { |c, i| c.stub(:index).and_return(i) }
        end

        it 'can be mapped' do
          pool.add_connections(connections)
          pool.map { |c| c.index }.should == [0, 1, 2]
        end

        it 'can be filtered' do
          pool.add_connections(connections)
          pool.select { |c| c.index % 2 == 0 }.should == [connections[0], connections[2]]
        end

        it 'raises a Errors::IOError when there are no connections' do
          expect { pool.select { } }.to raise_error(Errors::IOError)
        end
      end
    end
  end
end