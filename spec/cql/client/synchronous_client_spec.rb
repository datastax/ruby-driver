# encoding: utf-8

require 'spec_helper'
require 'cql/client/client_shared'


module Cql
  module Client
    describe SynchronousClient do
      let :client do
        described_class.new(async_client)
      end

      let :async_client do
        stub(:async_client)
      end

      let :future do
        stub(:future, get: nil)
      end

      describe '#connect' do
        it 'calls #connect on the async client and waits for the result' do
          async_client.should_receive(:connect).and_return(future)
          future.should_receive(:get)
          client.connect
        end

        it 'returns self' do
          async_client.stub(:connect).and_return(future)
          client.connect.should equal(client)
        end
      end

      describe '#close' do
        it 'calls #close on the async client and waits for the result' do
          async_client.should_receive(:close).and_return(future)
          future.should_receive(:get)
          client.close
        end

        it 'returns self' do
          async_client.stub(:close).and_return(future)
          client.close.should equal(client)
        end
      end

      describe '#connected?' do
        it 'delegates to the async client' do
          async_client.stub(:connected?).and_return(true)
          client.connected?.should be_true
          async_client.stub(:connected?).and_return(false)
          client.connected?.should be_false
        end
      end

      describe '#keyspace' do
        it 'delegates to the async client' do
          async_client.stub(:keyspace).and_return('foo')
          client.keyspace.should == 'foo'
        end
      end

      describe '#use' do
        it 'calls #use on the async client and waits for the result' do
          async_client.should_receive(:use).with('foo').and_return(future)
          future.should_receive(:get)
          client.use('foo')
        end
      end

      describe '#execute' do
        it 'calls #execute on the async client and waits for, and returns the result' do
          result = stub(:result)
          async_client.stub(:execute).with('SELECT * FROM something', :one).and_return(future)
          future.stub(:get).and_return(result)
          client.execute('SELECT * FROM something', :one).should equal(result)
        end
      end

      describe '#prepare' do
        it 'calls #prepare on the async client, waits for the result and returns a SynchronousFuture' do
          result = stub(:result)
          metadata = stub(:metadata)
          async_statement = stub(:async_statement, metadata: metadata)
          another_future = stub(:another_future)
          async_client.stub(:prepare).with('SELECT * FROM something').and_return(future)
          future.stub(:get).and_return(async_statement)
          statement = client.prepare('SELECT * FROM something')
          async_statement.should_receive(:execute).and_return(another_future)
          another_future.stub(:get).and_return(result)
          statement.execute.should equal(result)
          statement.metadata.should equal(metadata)
        end
      end

      describe '#async' do
        it 'returns an asynchronous client' do
          client.async.should equal(async_client)
        end
      end
    end
  end
end