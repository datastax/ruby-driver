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
          Protocol::RowsResultResponse.new(rows, metadata, nil, nil)
        end

        let :raw_rows_response do
          Protocol::RawRowsResultResponse.new(2, Protocol::CqlByteBuffer.new("\x00\x00\x00\x02\x00\x00\x00\x01a\x00\x00\x00\x01b"), nil, nil)
        end

        let :void_response do
          Protocol::VoidResultResponse.new(nil)
        end

        let :prepared_response do
          Protocol::PreparedResultResponse.new("\x2a", metadata, nil, nil)
        end

        let :error_response do
          Protocol::ErrorResponse.new(0xbad, 'Bork')
        end

        let :detailed_error_response do
          Protocol::DetailedErrorResponse.new(0xbad, 'Bork', {:cl => :quorum, :received => 1, :blockfor => 1, :write_type => 'SINGLE'})
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

        it 'transforms a RowsResultResponse to a QueryResult' do
          result = run(rows_response)
          result.should have(3).items
        end

        it 'transforms a RawRowsResultResponse to a LazyQueryResult, and mixes in the specified metadata' do
          metadata = [['ks', 'tbl', 'col', :varchar]]
          connection.stub(:send_request).and_return(Future.resolved(raw_rows_response))
          result = runner.execute(connection, request, nil, metadata).value
          result.to_a.should == [{'col' => 'a'}, {'col' => 'b'}]
        end

        it 'transforms a VoidResultResponse to a VoidResult' do
          result = run(void_response)
          result.should be_a(VoidResult)
        end

        it 'transforms an AuthenticateResponse to an authentication required object' do
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
            run(error_response, Protocol::QueryRequest.new('SELECT * FROM everything', nil, nil, :all))
          rescue QueryError => e
            e.cql.should == 'SELECT * FROM everything'
          else
            fail('No error was raised')
          end
        end

        it 'sets the #details field of QueryError when the response has details' do
          begin
            run(detailed_error_response)
          rescue QueryError => e
            e.details.should == detailed_error_response.details
          else
            fail('No error was raised')
          end
        end

        it 'transforms a SupportedResponse into its hash of supported options' do
          result = run(Protocol::SupportedResponse.new('CQL_VERSION' => %w[9.9.9], 'COMPRESSION' => %w[fractal quantum]))
          result.should eql('CQL_VERSION' => %w[9.9.9], 'COMPRESSION' => %w[fractal quantum])
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

          it 'returns a QueryResult that knows its trace ID' do
            connection.stub(:send_request).with(request, anything).and_return(Future.resolved(Protocol::RowsResultResponse.new(rows, metadata, nil, trace_id)))
            response = runner.execute(connection, request).value
            response.trace_id.should == trace_id
          end

          it 'returns a VoidResult that knows its trace ID' do
            connection.stub(:send_request).with(request, anything).and_return(Future.resolved(Protocol::VoidResultResponse.new(trace_id)))
            response = runner.execute(connection, request).value
            response.trace_id.should == trace_id
          end
        end

        context 'when the response has a paging state' do
          it 'returns a QueryResult that knows its paging state' do
            connection.stub(:send_request).with(request, anything).and_return(Future.resolved(Protocol::RowsResultResponse.new(rows, metadata, 'foobaz', nil)))
            response = runner.execute(connection, request).value
            response.paging_state.should == 'foobaz'
          end

          it 'returns a LazyQueryResult that knows its paging state' do
            metadata = [['ks', 'tbl', 'col', :varchar]]
            connection.stub(:send_request).with(request, anything).and_return(Future.resolved(Protocol::RawRowsResultResponse.new(2, Protocol::CqlByteBuffer.new("\x00\x00\x00\x02\x00\x00\x00\x01a\x00\x00\x00\x01b"), 'bazbuzz', nil)))
            response = runner.execute(connection, request, nil, metadata).value
            response.paging_state.should == 'bazbuzz'
          end
        end
      end
    end
  end
end