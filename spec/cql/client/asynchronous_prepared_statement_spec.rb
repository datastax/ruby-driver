# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe AsynchronousPreparedStatement do
      let :statement do
        described_class.new(connection, statement_id, raw_metadata)
      end

      let :connection do
        stub(:connection)
      end

      let :statement_id do
        "\x2a"
      end

      let :raw_metadata do
        [
          ['my_keyspace', 'my_table', 'my_column', :int],
          ['my_keyspace', 'my_table', 'my_other_column', :text],
        ]
      end

      let :rows do
        [
          {'my_column' => 11, 'my_other_column' => 'hello'},
          {'my_column' => 22, 'my_other_column' => 'foo'},
          {'my_column' => 33, 'my_other_column' => 'bar'},
        ]
      end

      describe '#metadata' do
        it 'returns the interpreted metadata' do
          statement.metadata.should be_a(ResultMetadata)
          statement.metadata['my_column'].should be_a(ColumnMetadata)
        end
      end

      describe '#execute' do
        it 'creates and sends an EXECUTE request' do
          expected_request = Cql::Protocol::ExecuteRequest.new(statement_id, raw_metadata, [11, 'hello'], :one)
          connection.should_receive(:send_request).with(expected_request)
          statement.execute(11, 'hello', :one)
        end

        it 'returns a future that resolves to a QueryResult' do
          request = Cql::Protocol::ExecuteRequest.new(statement_id, raw_metadata, [11, 'hello'], :two)
          response = Cql::Protocol::RowsResultResponse.new(rows, raw_metadata)
          connection.stub(:send_request).with(request).and_return(Future.completed(response))
          result = statement.execute(11, 'hello', :two).get
          result.metadata['my_other_column'].should == ColumnMetadata.new('my_keyspace', 'my_table', 'my_other_column', :text)
          result.first.should == {'my_column' => 11, 'my_other_column' => 'hello'}
        end

        it 'returns a failed future when the number of arguments is wrong' do
          f1 = statement.execute(11, :one)
          f2 = statement.execute(11, 'foo', 22, :one)
          expect { f1.get }.to raise_error
          expect { f2.get }.to raise_error
        end
      end
    end
  end
end