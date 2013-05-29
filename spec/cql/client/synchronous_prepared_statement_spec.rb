# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe SynchronousPreparedStatement do
      let :statement do
        described_class.new(async_statement)
      end

      let :async_statement do
        stub(:async_statement, metadata: metadata)
      end

      let :metadata do
        stub(:metadata)
      end

      let :future do
        stub(:future, get: nil)
      end

      describe '#metadata' do
        it 'returns the async statement\'s metadata' do
          statement.metadata.should equal(async_statement.metadata)
        end
      end

      describe '#execute' do
        it 'it calls #execute on the async statement and waits for the result' do
          result = stub(:result)
          async_statement.should_receive(:execute).with('one', 'two', :three).and_return(future)
          future.stub(:get).and_return(result)
          statement.execute('one', 'two', :three).should equal(result)
        end
      end
    end
  end
end