# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe AsynchronousPreparedStatement do
      let :connection_manager do
        ConnectionManager.new
      end

      let :logger do
        NullLogger.new
      end

      let :raw_metadata do
        [
          ['my_keyspace', 'my_table', 'my_column', :int],
          ['my_keyspace', 'my_table', 'my_other_column', :text],
        ]
      end

      let :raw_result_metadata do
        raw_metadata + [
          ['my_keyspace', 'my_table', 'a_third_column', :double],
        ]
      end

      let :rows do
        [
          {'my_column' => 11, 'my_other_column' => 'hello', 'a_third_column' => 0.0},
          {'my_column' => 22, 'my_other_column' => 'foo', 'a_third_column' => 0.0},
          {'my_column' => 33, 'my_other_column' => 'bar', 'a_third_column' => 0.0},
        ]
      end

      let :raw_rows do
        buffer = Protocol::CqlByteBuffer.new("\x00\x00\x00\x03")
        buffer << "\x00\x00\x00\x04\x00\x00\x00\x0b"
        buffer << "\x00\x00\x00\x05hello"
        buffer << "\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00"
        buffer << "\x00\x00\x00\x04\x00\x00\x00\x18"
        buffer << "\x00\x00\x00\x03foo"
        buffer << "\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00"
        buffer << "\x00\x00\x00\x04\x00\x00\x00\x21"
        buffer << "\x00\x00\x00\x03bar"
        buffer << "\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00"
        buffer
      end

      let :cql do
        'SELECT * FROM my_table'
      end

      let :connections do
        [
          FakeConnection.new('h0.example.com', 1234, 42),
          FakeConnection.new('h1.example.com', 1234, 42),
          FakeConnection.new('h2.example.com', 1234, 42),
        ]
      end

      let :protocol_version do
        'v2'
      end

      def handle_request(connection, request, timeout)
        case request
        when Protocol::PrepareRequest
          statement_id = Array.new(16) { [rand(255)].pack('c') }.join('')
          connection[:last_prepared_statement_id] = statement_id
          Protocol::PreparedResultResponse.new(statement_id, raw_metadata, protocol_version == 'v1' ? nil : raw_result_metadata, nil)
        when Protocol::ExecuteRequest
          if request.request_metadata
            Protocol::RowsResultResponse.new(rows, raw_metadata, request.paging_state ? 'page2' : nil, nil)
          else
            Protocol::RawRowsResultResponse.new(protocol_version[1].to_i, raw_rows, request.paging_state ? 'page2' : nil, nil)
          end
        when Protocol::BatchRequest
          Protocol::VoidResultResponse.new(nil)
        else
          raise %(Unexpected request: #{request})
        end
      end

      before do
        connections.each do |c|
          c.handle_request { |r, t| handle_request(c, r, t) }
        end
        connection_manager.add_connections(connections)
      end

      describe '.prepare' do
        it 'prepares a statement on all connections' do
          f = described_class.prepare(cql, ExecuteOptionsDecoder.new(:one), connection_manager, logger)
          f.value
          connections.each do |c|
            c.requests.should include(Protocol::PrepareRequest.new(cql))
          end
        end

        it 'returns a prepared statement object' do
          f = described_class.prepare(cql, ExecuteOptionsDecoder.new(:two), connection_manager, logger)
          f.value.should be_a(PreparedStatement)
        end

        it 'returns a failed future when something goes wrong in the preparation' do
          connections.each(&:close)
          f = described_class.prepare(cql, ExecuteOptionsDecoder.new(:three), connection_manager, logger)
          expect { f.value }.to raise_error(NotConnectedError)
        end

        it 'returns a failed future if the preparation results in an error' do
          connections.each do |connection|
            connection.stub(:send_request).and_return(Future.resolved(Protocol::ErrorResponse.new(99, 'bork')))
          end
          f = described_class.prepare(cql, :quorum, connection_manager, logger)
          expect { f.value }.to raise_error('bork')
        end
      end

      describe '#metadata' do
        let :statement do
          described_class.prepare(cql, ExecuteOptionsDecoder.new(:all), connection_manager, logger).value
        end

        it 'returns the interpreted metadata' do
          statement.metadata.should be_a(ResultMetadata)
          statement.metadata['my_column'].should be_a(ColumnMetadata)
        end
      end

      describe '#result_metadata' do
        let :statement do
          described_class.prepare(cql, ExecuteOptionsDecoder.new(:all), connection_manager, logger).value
        end

        it 'returns the interpreted result metadata' do
          statement.result_metadata.should be_a(ResultMetadata)
          statement.result_metadata['a_third_column'].should be_a(ColumnMetadata)
        end

        it 'is nil when there is no result metadata' do
          protocol_version.replace('v1')
          statement.result_metadata.should be_nil
        end
      end

      describe '#execute' do
        let :statement do
          described_class.prepare(cql, ExecuteOptionsDecoder.new(:local_quorum), connection_manager, logger).value
        end

        it 'executes itself on one of the connections' do
          statement.execute(11, 'hello').value
          requests = connections.flat_map(&:requests).select { |r| r.is_a?(Protocol::ExecuteRequest) }
          requests.should have(1).item
          requests.first.metadata.should == raw_metadata
          requests.first.values.should == [11, 'hello']
        end

        it 'uses the right statement ID for the connection' do
          statement.execute(11, 'hello').value
          connection, request = connections.map { |c| [c, c.requests.find { |r| r.is_a?(Protocol::ExecuteRequest) }] }.find { |c, r| r }
          request.id.should == connection[:last_prepared_statement_id]
        end

        it 'sends the default consistency level' do
          statement.execute(11, 'hello').value
          request = connections.flat_map(&:requests).find { |r| r.is_a?(Protocol::ExecuteRequest) }
          request.consistency.should == :local_quorum
        end

        it 'sends the consistency given as last argument' do
          statement.execute(11, 'hello', :two).value
          request = connections.flat_map(&:requests).find { |r| r.is_a?(Protocol::ExecuteRequest) }
          request.consistency.should == :two
        end

        it 'sends the consistency given as an option' do
          statement.execute(11, 'hello', consistency: :two).value
          request = connections.flat_map(&:requests).find { |r| r.is_a?(Protocol::ExecuteRequest) }
          request.consistency.should == :two
        end

        it 'sends the serial consistency given as an option' do
          statement.execute(11, 'hello', serial_consistency: :local_serial).value
          request = connections.flat_map(&:requests).find { |r| r.is_a?(Protocol::ExecuteRequest) }
          request.serial_consistency.should == :local_serial
        end

        it 'asks the server not to send metadata' do
          statement.execute(11, 'hello', consistency: :two).value
          request = connections.flat_map(&:requests).find { |r| r.is_a?(Protocol::ExecuteRequest) }
          request.request_metadata.should be_false
        end

        it 'passes the metadata to the request runner' do
          response = statement.execute(11, 'hello', consistency: :two).value
          response.count.should == 3
        end

        it 'uses the specified timeout' do
          sent_timeout = nil
          connections.each do |c|
            c.handle_request do |r, t|
              sent_timeout = t
              handle_request(c, r, t)
            end
          end
          statement.execute(11, 'hello', timeout: 3).value
          sent_timeout.should == 3
        end

        context 'when paging' do
          it 'sends the page size given as an option' do
            statement.execute(11, 'hello', page_size: 10).value
            request = connections.flat_map(&:requests).find { |r| r.is_a?(Protocol::ExecuteRequest) }
            request.page_size.should == 10
          end

          it 'sends the page size and paging state given as options' do
            statement.execute(11, 'hello', page_size: 10, paging_state: 'foo').value
            request = connections.flat_map(&:requests).find { |r| r.is_a?(Protocol::ExecuteRequest) }
            request.page_size.should == 10
            request.paging_state.should == 'foo'
          end

          it 'returns a result which can load the next page' do
            result = statement.execute(11, 'foo', page_size: 2).value
            result.next_page.value
            request = connections.flat_map(&:requests).find { |r| r.is_a?(Protocol::ExecuteRequest) }
            request.paging_state.should == result.paging_state
          end

          it 'returns a result which knows when there are no more pages' do
            result = statement.execute(11, 'foo', page_size: 2).value
            result.should be_last_page
          end
        end

        context 'when it receives a new connection from the connection manager' do
          let :new_connection do
            FakeConnection.new('h3.example.com', 1234, 5)
          end

          before do
            statement
            new_connection.handle_request { |r, t| handle_request(new_connection, r, t) }
            connections.each(&:close)
            connection_manager.add_connections([new_connection])
          end

          it 'prepares itself on the connection' do
            statement.execute(11, 'hello')
            new_connection.requests.should include(Protocol::PrepareRequest.new(cql))
            execute_request = new_connection.requests.find { |r| r.is_a?(Protocol::ExecuteRequest) }
            execute_request.metadata.should == raw_metadata
            execute_request.values.should == [11, 'hello']
          end

          it 'logs a message with the host ID, host address and its own ID' do
            new_connection[:host_id] = Uuid.new('a4a70900-24e1-11df-8924-001ff3591711')
            logger.stub(:debug)
            statement.execute(11, 'hello')
            logger.should have_received(:debug).with(/Statement [0-9a-f]{32} prepared on node a4a70900-24e1-11df-8924-001ff3591711 \(h3.example.com:1234\)/).once
          end
        end

        it 'returns a future that resolves to the result' do
          f = statement.execute(11, 'hello')
          query_result = f.value
          query_result.metadata['my_other_column'].should == ColumnMetadata.new('my_keyspace', 'my_table', 'my_other_column', :text)
          query_result.first.should == rows.first
        end

        it 'returns a failed future when the number of arguments is wrong' do
          f1 = statement.execute(11, :one)
          f2 = statement.execute(11, 'foo', 22, :one)
          expect { f1.value }.to raise_error(NoMethodError)
          expect { f2.value }.to raise_error(ArgumentError)
        end

        it 'sets the trace flag' do
          tracing = false
          connections.each do |c|
            c.handle_request do |r, t|
              if r.is_a?(Protocol::ExecuteRequest)
                tracing = r.trace
              end
              handle_request(c, r, t)
            end
          end
          statement.execute(11, 'hello', trace: true).value
          tracing.should be_true
        end
      end

      describe '#batch' do
        let :statement do
          described_class.prepare('UPDATE x SET y = ? WHERE z = ?', ExecuteOptionsDecoder.new(:one), connection_manager, logger).value
        end

        def requests
          connections.flat_map(&:requests).select { |r| r.is_a?(Protocol::BatchRequest) }
        end

        context 'when called witout a block' do
          it 'returns a batch' do
            batch = statement.batch
            batch.add(1, 'foo')
            batch.execute.value
            requests.first.should be_a(Protocol::BatchRequest)
          end

          it 'creates a batch of the right type' do
            batch = statement.batch(:unlogged)
            batch.add(1, 'foo')
            batch.execute.value
            requests.first.type.should == Protocol::BatchRequest::UNLOGGED_TYPE
          end

          it 'passes the options to the batch' do
            batch = statement.batch(trace: true)
            batch.add(1, 'foo')
            batch.execute.value
            requests.first.trace.should be_true
          end
        end

        context 'when called with a block' do
          it 'yields and executes a batch' do
            f = statement.batch do |batch|
              batch.add(5, 'foo')
              batch.add(6, 'bar')
            end
            f.value
            requests.first.should be_a(Protocol::BatchRequest)
          end

          it 'passes the options to the batch\'s #execute' do
            f = statement.batch(:unlogged, trace: true) do |batch|
              batch.add(4, 'baz')
            end
            f.value
            requests.first.trace.should be_true
          end
        end
      end

      describe '#add_to_batch' do
        let :statement do
          described_class.prepare('UPDATE x SET y = ? WHERE z = ?', ExecuteOptionsDecoder.new(:one), connection_manager, logger).value
        end

        let :batch do
          double(:batch)
        end

        let :additions do
          []
        end

        before do
          connections.pop(2)
          batch.stub(:add_prepared) do |*args|
            additions << args
          end
        end

        it 'calls #add_prepared with the statement ID, metadata and bound variables' do
          statement.add_to_batch(batch, connections.first, [11, 'foo'])
          statement_id, metadata, bound_args = additions.first
          statement_id.should == connections.first[:last_prepared_statement_id]
          metadata.should == raw_metadata
          bound_args.should == [11, 'foo']
        end

        it 'raises an error when the number of bound arguments is not right' do
          expect { statement.add_to_batch(batch, connections.first, [11, 'foo', 22]) }.to raise_error(ArgumentError)
        end

        it 'raises an error when the statement has not been prepared on the specified connection' do
          connection = double(:connection)
          connection.stub(:[]).with(statement).and_return(nil)
          expect { statement.add_to_batch(batch, connection, [11, 'foo']) }.to raise_error(NotPreparedError)
        end
      end
    end

    describe SynchronousPreparedStatement do
      let :statement do
        described_class.new(async_statement)
      end

      let :async_statement do
        double(:async_statement, metadata: metadata, result_metadata: result_metadata)
      end

      let :metadata do
        double(:metadata)
      end

      let :result_metadata do
        double(:result_metadata)
      end

      let :promise do
        Promise.new
      end

      let :future do
        promise.future
      end

      describe '#metadata' do
        it 'returns the async statement\'s metadata' do
          statement.metadata.should equal(async_statement.metadata)
        end
      end

      describe '#result_metadata' do
        it 'returns the async statement\'s result metadata' do
          statement.result_metadata.should equal(async_statement.result_metadata)
        end
      end

      describe '#execute' do
        it 'it calls #execute on the async statement and waits for the result' do
          result = double(:result)
          async_statement.should_receive(:execute).with('one', 'two', :three).and_return(future)
          promise.fulfill(result)
          statement.execute('one', 'two', :three).should equal(result)
        end

        it 'wraps AsynchronousPagedQueryResult in a synchronous wrapper' do
          request = double(:request, values: ['one', 'two'])
          async_result = double(:result, paging_state: 'somepagingstate')
          options = {:page_size => 10}
          async_statement.stub(:execute).and_return(Future.resolved(AsynchronousPreparedPagedQueryResult.new(async_statement, request, async_result, options)))
          result1 = statement.execute('one', 'two', options)
          result2 = result1.next_page
          async_statement.should have_received(:execute).with('one', 'two', page_size: 10, paging_state: 'somepagingstate')
          result2.should be_a(SynchronousPagedQueryResult)
        end
      end

      describe '#batch' do
        let :batch do
          double(:batch)
        end

        context 'when called without a block' do
          it 'delegates to the asynchronous statement and wraps the returned object in a synchronous wrapper' do
            async_statement.stub(:batch).with(:unlogged, trace: true).and_return(batch)
            batch.stub(:execute).and_return(Cql::Future.resolved(VoidResult.new))
            b = statement.batch(:unlogged, trace: true)
            b.execute.should be_a(VoidResult)
          end
        end

        context 'when called with a block' do
          it 'delegates to the asynchronous statement' do
            async_statement.stub(:batch).with(:counter, trace: true).and_yield(batch).and_return(Cql::Future.resolved(VoidResult.new))
            yielded_batch = nil
            statement.batch(:counter, trace: true) { |b| yielded_batch = b }
            yielded_batch.should equal(batch)
          end

          it 'waits for the operation to complete' do
            async_statement.stub(:batch).with(:counter, anything).and_yield(batch).and_return(Cql::Future.resolved(VoidResult.new))
            result = statement.batch(:counter) { |b| }
            result.should be_a(VoidResult)
          end
        end
      end

      describe '#add_to_batch' do
        it 'delegates to the async statement' do
          batch = double(:batch)
          connection = double(:connection)
          bound_arguments = [1, 2, 3]
          async_statement.stub(:add_to_batch)
          statement.add_to_batch(batch, connection, bound_arguments)
          async_statement.should have_received(:add_to_batch).with(batch, connection, bound_arguments)
        end
      end

      describe '#pipeline' do
        it 'executes the statement multiple times and waits for all the results' do
          result1 = double(:result1)
          result2 = double(:result2)
          async_statement.stub(:execute).with('one', 'two', :three).and_return(Future.resolved(result1))
          async_statement.stub(:execute).with('four', 'file', :all).and_return(Future.resolved(result2))
          results = statement.pipeline do |p|
            p.execute('one', 'two', :three)
            p.execute('four', 'file', :all)
          end
          results.should eql([result1, result2])
        end

        it 'does nothing when statements are executed' do
          statement.pipeline { |p| }.should == []
        end
      end

      describe '#async' do
        it 'returns an asynchronous statement' do
          statement.async.should equal(async_statement)
        end
      end

      context 'when exceptions are raised' do
        it 'replaces the backtrace of the asynchronous call to make it less confusing' do
          error = CqlError.new('Bork')
          error.set_backtrace(['Hello', 'World'])
          future.stub(:value).and_raise(error)
          async_statement.stub(:execute).and_return(future)
          begin
            statement.execute('SELECT * FROM something')
          rescue CqlError => e
            e.backtrace.first.should match(%r{/prepared_statement.rb:\d+:in `execute'})
          end
        end

        it 'does not replace the backtrace of non-CqlError errors' do
          future.stub(:value).and_raise('Bork')
          async_statement.stub(:execute).and_return(future)
          begin
            statement.execute('SELECT * FROM something')
          rescue => e
            e.backtrace.first.should_not match(%r{/prepared_statement.rb:\d+:in `execute'})
          end
        end
      end
    end
  end
end
