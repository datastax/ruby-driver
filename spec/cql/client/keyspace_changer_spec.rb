# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe KeyspaceChanger do
      let :keyspace_changer do
        described_class.new
      end

      let :connection do
        double(:connection)
      end

      describe '#use_keyspace' do
        it 'sends a query request with a USE statement' do
          connection.stub(:send_request).with(Protocol::QueryRequest.new('USE important_stuff', nil, nil, :one), nil).and_return(Future.resolved)
          f = keyspace_changer.use_keyspace(connection, 'important_stuff')
          connection.should have_received(:send_request)
        end

        it 'accepts quoted keyspace names' do
          connection.stub(:send_request).with(Protocol::QueryRequest.new('USE "ImportantStuff"', nil, nil, :one), nil).and_return(Future.resolved)
          f = keyspace_changer.use_keyspace(connection, '"ImportantStuff"')
          connection.should have_received(:send_request)
        end

        context 'returns a future that' do
          it 'immediately resolves to the given connection when the keyspace is nil' do
            f = keyspace_changer.use_keyspace(connection, nil)
            f.value.should equal(connection)
          end

          it 'fails with an InvalidKeyspaceNameError when the keyspace name is invalid' do
            f = keyspace_changer.use_keyspace(connection, 'TRUNCATE important_stuff')
            expect { f.value }.to raise_error(InvalidKeyspaceNameError)
          end

          it 'resolves to the given connection' do
            connection.stub(:send_request).with(Protocol::QueryRequest.new('USE important_stuff', nil, nil, :one), nil).and_return(Future.resolved)
            f = keyspace_changer.use_keyspace(connection, 'important_stuff')
            f.value.should equal(connection)
          end
        end
      end
    end
  end
end