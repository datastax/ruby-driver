# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe AsynchronousBatch do
      let :batch do
        described_class.new(:unlogged, execute_options_decoder, connection_manager)
      end

      let :execute_options_decoder do
        ExecuteOptionsDecoder.new(:two)
      end

      let :connection_manager do
        double(:connection_manager)
      end

      let :connection do
        double(:connection)
      end

      describe '#initialize' do
        it 'raises an error when the type is not :logged, :unlogged or :counter' do
          expect { described_class.new(:foobar, execute_options_decoder, connection_manager) }.to raise_error(ArgumentError)
        end
      end

      describe '#add' do
        it 'returns nil' do
          result = batch.add('UPDATE x SET y = 1 WHER z = 2')
          result.should be_nil
        end
      end

      describe '#execute' do
        let :requests do
          []
        end

        def last_request
          requests.last[0]
        end

        def last_timeout
          requests.last[1]
        end

        let :prepared_statement do
          double(:prepared_statement)
        end

        let :metadata do
          [['ks', 'tbl', 'col1', :bigint], ['ks', 'tbl', 'col2', :text]]
        end

        before do
          connection_manager.stub(:random_connection).and_return(connection)
          connection.stub(:send_request) do |request, timeout|
            requests << [request, timeout]
            Future.resolved(Protocol::VoidResultResponse.new(nil))
          end
        end

        it 'creates a BATCH request and executes it on a random connection' do
          batch.execute.value
          connection.should have_received(:send_request).with(an_instance_of(Protocol::BatchRequest), nil)
        end

        it 'creates a BATCH request from the added parts' do
          prepared_statement.stub(:add_to_batch) do |batch_request, connection, bound_args|
            batch_request.add_prepared('XXXXXXXXXXXXXXXX', metadata, bound_args)
          end
          prepared_statement.stub(:metadata).and_return(metadata)
          batch.add('UPDATE x SET y = 1 WHERE z = 2')
          batch.add('UPDATE x SET y = 2 WHERE z = ?', 3)
          batch.add(prepared_statement, 3, 'foo')
          batch.execute.value
          encoded_frame = last_request.write(1, Protocol::CqlByteBuffer.new)
          encoded_frame.to_s.should include('UPDATE x SET y = 1 WHERE z = 2')
          encoded_frame.to_s.should include('UPDATE x SET y = 2 WHERE z = ?')
          encoded_frame.to_s.should include(Protocol::QueryRequest.encode_values(Protocol::CqlByteBuffer.new, [3], nil))
          encoded_frame.to_s.should include('XXXXXXXXXXXXXXXX')
          encoded_frame.to_s.should include(Protocol::ExecuteRequest.encode_values(Protocol::CqlByteBuffer.new, metadata, [3, 'foo']))
        end

        it 'resets the batch so that it can be used again' do
          batch.add('UPDATE x SET y = 1 WHERE z = 2')
          batch.execute.value
          batch.add('UPDATE x SET y = 2 WHERE z = 3')
          batch.execute.value
          first_request, second_request = requests.map { |r| r[0].write(1, Protocol::CqlByteBuffer.new).to_s }
          first_request.should include('UPDATE x SET y = 1 WHERE z = 2')
          second_request.should include('UPDATE x SET y = 2 WHERE z = 3')
          second_request.should_not include('UPDATE x SET y = 1 WHERE z = 2')
        end

        it 'uses the provided type hints' do
          batch.add('UPDATE x SET y = 2 WHERE z = ?', 3, type_hints: [:int])
          batch.execute.value
          encoded_frame = last_request.write(1, Protocol::CqlByteBuffer.new)
          encoded_frame.to_s.should include(Protocol::QueryRequest.encode_values(Protocol::CqlByteBuffer.new, [3], [:int]))
        end

        it 'tries again when a prepared statement raises NotPreparedError' do
          connection1 = double(:connection1)
          connection2 = double(:connection2)
          connection2.stub(:send_request).and_return(Cql::Future.resolved(Protocol::VoidResultResponse.new(nil)))
          connection_manager.stub(:random_connection).and_return(connection1, connection2)
          prepared_statement.stub(:add_to_batch).with(anything, connection1, anything).and_raise(NotPreparedError)
          prepared_statement.stub(:add_to_batch).with(anything, connection2, anything)
          batch.add(prepared_statement, 3, 'foo')
          expect { batch.execute.value }.to_not raise_error
        end

        it 'gives up when the prepared statement has raised NotPreparedError three times' do
          prepared_statement.stub(:add_to_batch).with(anything, connection, anything).and_raise(NotPreparedError)
          batch.add(prepared_statement, 3, 'foo')
          expect { batch.execute.value }.to raise_error(NotPreparedError)
          prepared_statement.should have_received(:add_to_batch).exactly(3).times
        end

        it 'returns a future that resolves to the response' do
          f = batch.execute
          f.value.should equal(VoidResult::INSTANCE)
        end

        it 'accepts a timeout' do
          batch.execute(timeout: 10).value
          last_timeout.should == 10
        end

        it 'accepts a consistency' do
          batch.execute(consistency: :three).value
          last_request.consistency.should == :three
        end

        it 'accepts consistency as a symbol' do
          batch.execute(:three).value
          last_request.consistency.should == :three
        end

        it 'uses the default consistency' do
          batch.execute.value
          last_request.consistency.should == :two
        end

        it 'enables tracing' do
          batch.execute(trace: true).value
          last_request.trace.should be_true
        end

        it 'creates a batch of the right type' do
          b1 = described_class.new(:logged, execute_options_decoder, connection_manager)
          b2 = described_class.new(:unlogged, execute_options_decoder, connection_manager)
          b3 = described_class.new(:counter, execute_options_decoder, connection_manager)
          b1.execute.value
          last_request.type.should == Protocol::BatchRequest::LOGGED_TYPE
          b2.execute.value
          last_request.type.should == Protocol::BatchRequest::UNLOGGED_TYPE
          b3.execute.value
          last_request.type.should == Protocol::BatchRequest::COUNTER_TYPE
        end

        it 'uses the options given in the constructor' do
          b = described_class.new(:unlogged, execute_options_decoder, connection_manager, timeout: 4)
          b.execute.value
          last_timeout.should == 4
        end

        it 'merges the options with the options given in the constructor' do
          b = described_class.new(:unlogged, execute_options_decoder, connection_manager, timeout: 4, trace: true)
          b.execute(trace: false).value
          last_timeout.should == 4
          last_request.trace.should be_false
        end
      end
    end

    describe SynchronousBatch do
      let :batch do
        described_class.new(asynchronous_batch)
      end

      let :asynchronous_batch do
        double(:asynchronous_batch)
      end

      before do
        asynchronous_batch.stub(:add)
        asynchronous_batch.stub(:execute).and_return(Cql::Future.resolved(VoidResult::INSTANCE))
      end

      describe '#async' do
        it 'returns the asynchronous batch' do
          batch.async.should equal(asynchronous_batch)
        end
      end

      describe '#add' do
        it 'delegates to the asynchronous batch' do
          batch.add('UPDATE x SET y = ? WHERE z = ?', 3, 4)
          asynchronous_batch.should have_received(:add).with('UPDATE x SET y = ? WHERE z = ?', 3, 4)
        end
      end

      describe '#execute' do
        it 'delegates to the asynchronous batch' do
          batch.execute(trace: true)
          asynchronous_batch.should have_received(:execute).with(trace: true)
        end

        it 'waits for the response' do
          batch.execute.should == VoidResult::INSTANCE
        end
      end
    end

    describe AsynchronousPreparedStatementBatch do
      let :prepared_statement_batch do
        described_class.new(prepared_statement, batch)
      end

      let :prepared_statement do
        double(:prepared_statement)
      end

      let :batch do
        double(:batch)
      end

      before do
        batch.stub(:add)
        batch.stub(:execute).and_return(Cql::Future.resolved(VoidResult::INSTANCE))
      end

      describe '#add' do
        it 'passes the statement and the given arguments to the batch' do
          prepared_statement_batch.add('foo', 3)
          batch.should have_received(:add).with(prepared_statement, 'foo', 3)
        end
      end

      describe '#execute' do
        it 'delegates to the batch' do
          result = prepared_statement_batch.execute(trace: true)
          batch.should have_received(:execute).with(trace: true)
          result.value.should == VoidResult::INSTANCE
        end
      end
    end

    describe SynchronousPreparedStatementBatch do
      let :batch do
        described_class.new(asynchronous_batch)
      end

      let :asynchronous_batch do
        double(:asynchronous_batch)
      end

      before do
        asynchronous_batch.stub(:add)
        asynchronous_batch.stub(:execute).and_return(Cql::Future.resolved(VoidResult::INSTANCE))
      end

      describe '#add' do
        it 'delegates to the async batch' do
          batch.add(3, 'foo', 9)
          asynchronous_batch.should have_received(:add).with(3, 'foo', 9)
        end
      end

      describe '#execute' do
        it 'delegates to the async batch' do
          batch.execute(trace: true)
          asynchronous_batch.should have_received(:execute).with(trace: true)
        end

        it 'waits for the response' do
          batch.execute.should == VoidResult::INSTANCE
        end
      end
    end
  end
end