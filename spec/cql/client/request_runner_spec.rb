# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe RequestRunner do
      let :runner do
        described_class.new
      end

      let :connection do
        double(:connection)
      end

      let :request do
        double(:request)
      end

      let :metadata do
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

      describe '#execute' do
        let :rows_response do
          Protocol::RowsResultResponse.new(rows, metadata, nil)
        end

        let :void_response do
          Protocol::VoidResultResponse.new(nil)
        end

        let :prepared_response do
          Protocol::PreparedResultResponse.new("\x2a", metadata, nil)
        end

        let :error_response do
          Protocol::ErrorResponse.new(0xbad, 'Bork')
        end

        let :authenticate_response do
          Protocol::AuthenticateResponse.new('TheAuthenticator')
        end

        let :set_keyspace_response do
          Protocol::SetKeyspaceResultResponse.new('some_keyspace', nil)
        end

        def run(response, rq=request)
          connection.stub(:send_request).and_return(Future.resolved(response))
          runner.execute(connection, rq).value
        end

        it 'executes the request' do
          connection.stub(:send_request).and_return(Future.resolved(rows_response))
          runner.execute(connection, request)
          connection.should have_received(:send_request)
        end

        it 'executes the request with the specified timeout' do
          connection.stub(:send_request).and_return(Future.resolved(rows_response))
          runner.execute(connection, request, 7)
          connection.should have_received(:send_request).with(request, 7)
        end

        it 'transforms a RowsResultResponse to a query result' do
          result = run(rows_response)
          result.should have(3).items
        end

        it 'transforms a VoidResultResponse to nil' do
          result = run(void_response)
          result.should be_nil
        end

        it 'transforms a AuthenticateResponse to an authentication required object' do
          result = run(authenticate_response)
          result.should be_a(AuthenticationRequired)
          result.authentication_class.should == 'TheAuthenticator'
        end

        it 'transforms a SetKeyspaceResultResponse into a keyspace changed object' do
          result = run(set_keyspace_response)
          result.should be_a(KeyspaceChanged)
          result.keyspace.should == 'some_keyspace'
        end

        it 'intercepts an ErrorResponse and fails the result future' do
          expect { run(error_response) }.to raise_error(QueryError)
        end

        it 'sets the #cql field of QueryError when the request is a query request' do
          begin
            run(error_response, Protocol::QueryRequest.new('SELECT * FROM everything', :all))
          rescue QueryError => e
            e.cql.should == 'SELECT * FROM everything'
          else
            fail('No error was raised')
          end
        end

        it 'transforms all other responses to nil' do
          result = run('hibbly hobbly')
          result.should be_nil
        end

        it 'allows the caller to transform unknown responses' do
          connection.stub(:send_request).and_return(Future.resolved('hibbly hobbly'))
          result = runner.execute(connection, request) { |response| response.reverse }.value
          result.should == 'hibbly hobbly'.reverse
        end

        context 'when the response has a trace ID' do
          let :trace_id do
            Uuid.new('63a26b40-3f02-11e3-9531-fb72eff05fbb')
          end

          let :session_rows do
            [{'session_id' => trace_id, 'coordinator' => IPAddr.new('127.0.0.1'), 'duration' => 1263, 'parameters' => {'query' => 'SELECT * FROM something'}, 'request' => 'Execute CQL3 query', 'started_at' => Time.now}]
          end

          let :event_rows do
            [
              {'session_id' => trace_id, 'event_id' => TimeUuid.new('a1028491-3f05-11e3-9531-fb72eff05fbb'), 'activity' => 'Parsing statement', 'source' => IPAddr.new('127.0.0.1'), 'source_elapsed' => 52, 'thread' => 'Native-Transport-Requests:126'},
              {'session_id' => trace_id, 'event_id' => TimeUuid.new('a1028492-3f05-11e3-9531-fb72eff05fbb'), 'activity' => 'Peparing statement', 'source' => IPAddr.new('127.0.0.1'), 'source_elapsed' => 54, 'thread' => 'Native-Transport-Requests:126'},
            ]
          end

          it 'returns a QueryResult that can load its trace' do
            connection.stub(:send_request) do |rq, timeout|
              if rq == request
                Future.resolved(Protocol::RowsResultResponse.new(rows, metadata, trace_id))
              elsif rq.cql == 'SELECT * FROM system_traces.sessions WHERE session_id = 63a26b40-3f02-11e3-9531-fb72eff05fbb'
                Future.resolved(Protocol::RowsResultResponse.new(session_rows, [:fake_metadata], nil))
              elsif rq.cql == 'SELECT * FROM system_traces.events WHERE session_id = 63a26b40-3f02-11e3-9531-fb72eff05fbb'
                Future.resolved(Protocol::RowsResultResponse.new(event_rows, [:fake_metadata], nil))
              end
            end
            response = runner.execute(connection, request).value
            trace = response.trace.value
            trace.cql.should == 'SELECT * FROM something'
          end

          it 'raises an error from #trace when the duration field of the trace is nil' do
            session_rows[0]['duration'] = nil
            connection.stub(:send_request) do |rq, timeout|
              if rq == request
                Future.resolved(Protocol::RowsResultResponse.new(rows, metadata, trace_id))
              elsif rq.cql == 'SELECT * FROM system_traces.sessions WHERE session_id = 63a26b40-3f02-11e3-9531-fb72eff05fbb'
                Future.resolved(Protocol::RowsResultResponse.new(session_rows, [:fake_metadata], nil))
              elsif rq.cql == 'SELECT * FROM system_traces.events WHERE session_id = 63a26b40-3f02-11e3-9531-fb72eff05fbb'
                Future.resolved(Protocol::RowsResultResponse.new(event_rows, [:fake_metadata], nil))
              end
            end
            response = runner.execute(connection, request).value
            expect { response.trace.value }.to raise_error(IncompleteTraceError)
          end
        end
      end
    end
  end
end