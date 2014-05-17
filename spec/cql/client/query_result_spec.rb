# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    shared_context 'query_result_setup' do
      let :metadata do
        [
          ['ks', 'tbl', 'col1', :varchar],
          ['ks', 'tbl', 'col2', :double],
        ]
      end

      let :trace_id do
        double(:trace_id)
      end

      let :rows do
        [double(:row1), double(:row2), double(:row3)]
      end
    end

    shared_examples 'query_result_shared' do
      describe '#metadata' do
        it 'wraps the raw metadata in a ResultMetadata' do
          result.metadata['col1'].should == ColumnMetadata.new('ks', 'tbl', 'col1', :varchar)
        end
      end

      describe '#each' do
        it 'yields each row' do
          yielded_rows = []
          result.each { |r| yielded_rows << r }
          yielded_rows.should == rows
        end

        it 'can be iterated multiple times' do
          yielded_rows = []
          result.each { |r| yielded_rows << r }
          result.each { |r| yielded_rows << r }
          yielded_rows.should == rows + rows
        end

        it 'is aliased as #each_row' do
          result.each_row { }
        end

        it 'returns an Enumerable when no block is given' do
          result.each.to_a.should == rows
        end
      end

      context 'when used as an Enumerable' do
        before do
          rows.each_with_index { |r, i| r.stub(:[]).with('col2').and_return(i.to_f) }
        end

        it 'transforms the rows' do
          result.map { |r| r['col2'] * 2 }.should == [0.0, 2.0, 4.0]
        end

        it 'filters the rows' do
          result.select { |r| r['col2'] > 0 }.should == rows.drop(1)
        end
      end
    end

    describe QueryResult do
      let :result do
        described_class.new(metadata, rows, trace_id, nil)
      end

      include_context 'query_result_setup'
      include_examples 'query_result_shared'

      describe '#empty?' do
        it 'returns true when there are no rows' do
          described_class.new(metadata, [], nil, nil).should be_empty
        end

        it 'returns false when there are rows' do
          result.should_not be_empty
        end
      end

      describe '#last_page?' do
        it 'returns true' do
          result.should be_last_page
        end
      end

      describe '#next_page' do
        it 'returns nil' do
          result.next_page.should be_nil
        end
      end
    end

    describe LazyQueryResult do
      let :result do
        described_class.new(metadata, lazy_rows, trace_id, nil)
      end

      let :lazy_rows do
        double(:lazy_rows)
      end

      include_context 'query_result_setup'

      before do
        lazy_rows.stub(:rows).and_return(nil)
        lazy_rows.stub(:materialize) do |md|
          raise '#materialize called twice' if lazy_rows.rows
          md.should == metadata
          lazy_rows.stub(:rows).and_return(rows)
          rows
        end
      end

      include_examples 'query_result_shared'

      describe '#empty?' do
        it 'returns true when there are no rows' do
          lazy_rows.stub(:materialize) do
            lazy_rows.stub(:rows).and_return([])
            []
          end
          result.should be_empty
        end

        it 'returns false when there are rows' do
          result.should_not be_empty
        end
      end
    end

    shared_examples 'paged_query_result' do
      let :metadata do
        ResultMetadata.new([['ks', 'tbl', 'col1', :varchar], ['ks', 'tbl', 'col2', :double]])
      end

      describe '#metadata' do
        it 'delegates to the wrapped result' do
          query_result.stub(:metadata).and_return(metadata)
          paged_query_result.metadata.should == metadata
        end
      end

      describe '#trace_id' do
        it 'delegates to the wrapped result' do
          query_result.stub(:trace_id).and_return('foobaz')
          paged_query_result.trace_id.should == 'foobaz'
        end
      end

      describe '#paging_state' do
        it 'delegates to the wrapped result' do
          query_result.stub(:paging_state).and_return('foobaz')
          paged_query_result.paging_state.should == 'foobaz'
        end
      end

      describe '#empty?' do
        it 'delegates to the wrapped result' do
          query_result.stub(:empty?).and_return(true)
          paged_query_result.should be_empty
        end
      end

      describe '#each' do
        it 'delegates to the wrapped result' do
          query_result.stub(:each) do |&block|
            block.call(:row1)
            block.call(:row2)
          end
          rows = paged_query_result.each_with_object([]) { |row, rows| rows << row }
          rows.should == [:row1, :row2]
        end
      end
    end

    shared_examples 'asynchronous_paged_query_result' do
      include_examples 'paged_query_result'

      describe '#last_page?' do
        it 'returns true when the result has no paging state' do
          query_result.stub(:paging_state).and_return(nil)
          paged_query_result.should be_last_page
        end

        it 'returns false when the result has a paging state' do
          paged_query_result.should_not be_last_page
        end
      end
    end

    describe AsynchronousQueryPagedQueryResult do
      let :paged_query_result do
        described_class.new(client, request, query_result, options)
      end

      let :client do
        double(:client)
      end

      let :request do
        double(:request, cql: 'SELECT * FROM something WHERE id = ?', values: ['foo'])
      end

      let :query_result do
        double(:query_result, paging_state: 'thepagingstate')
      end

      let :options do
        {:trace => true, :timeout => 3}
      end

      include_examples 'asynchronous_paged_query_result'

      describe '#next_page' do
        let :next_query_result do
          described_class.new(client, request, double(:next_query_result, paging_state: 'thenextpagingstate'), options)
        end

        let :last_query_result do
          described_class.new(client, request, double(:next_query_result, paging_state: nil), options)
        end

        before do
          client.stub(:execute).and_return(Future.resolved(next_query_result))
          client.stub(:execute).with(anything, anything, hash_including(paging_state: 'thenextpagingstate')).and_return(Future.resolved(last_query_result))
        end

        it 'calls the client and passes the paging state' do
          paged_query_result.next_page.value
          client.should have_received(:execute).with(anything, anything, hash_including(paging_state: 'thepagingstate'))
        end

        it 'calls the client and passes the options' do
          paged_query_result.next_page.value
          client.should have_received(:execute).with(anything, anything, hash_including(options))
        end

        it 'calls the client and passes the CQL' do
          paged_query_result.next_page.value
          client.should have_received(:execute).with(request.cql, anything, anything)
        end

        it 'calls the client and passes the bound values' do
          paged_query_result.next_page.value
          client.should have_received(:execute).with(anything, 'foo', anything)
        end

        it 'handles the case when there are multiple bound values' do
          request.stub(:values).and_return(['foo', 3, 'bar', 4])
          paged_query_result.next_page.value
          client.should have_received(:execute).with(anything, 'foo', 3, 'bar', 4, anything)
        end

        it 'handles the case when there are no bound values' do
          request.stub(:values).and_return(nil)
          paged_query_result.next_page.value
          client.should have_received(:execute).with(request.cql, an_instance_of(Hash))
        end

        it 'returns the result of the call' do
          f = paged_query_result.next_page
          f.value.should equal(next_query_result)
        end

        it 'returns nil when it is the last page' do
          f = paged_query_result.next_page.value.next_page.value.next_page
          f.value.should be_nil
        end
      end
    end

    describe AsynchronousPreparedPagedQueryResult do
      let :paged_query_result do
        described_class.new(statement, request, query_result, options)
      end

      let :statement do
        double(:statement)
      end

      let :request do
        double(:request, values: ['foo', 3])
      end

      let :query_result do
        double(:query_result, paging_state: 'thepagingstate')
      end

      let :options do
        {:trace => true, :timeout => 3}
      end

      include_examples 'asynchronous_paged_query_result'

      describe '#next_page' do
        let :next_query_result do
          described_class.new(statement, request, double(:next_query_result, paging_state: 'thenextpagingstate'), options)
        end

        let :last_query_result do
          described_class.new(statement, request, double(:next_query_result, paging_state: nil), options)
        end

        before do
          statement.stub(:execute).and_return(Future.resolved(next_query_result))
          statement.stub(:execute).with(anything, anything, hash_including(paging_state: 'thenextpagingstate')).and_return(Future.resolved(last_query_result))
        end

        it 'calls the statement and passes the paging state' do
          paged_query_result.next_page.value
        end

        it 'calls the statement and passes the options' do
          paged_query_result.next_page.value
          statement.should have_received(:execute).with(anything, anything, hash_including(options))
        end

        it 'calls the statement and passes the bound values' do
          paged_query_result.next_page.value
          statement.should have_received(:execute).with('foo', 3, anything)
        end

        it 'handles the case when there are no bound values' do
          request.stub(:values).and_return(nil)
          paged_query_result.next_page.value
          statement.should have_received(:execute).with(an_instance_of(Hash))
        end

        it 'returns the result of the call' do
          f = paged_query_result.next_page
          f.value.should equal(next_query_result)
        end

        it 'returns nil when it is the last page' do
          f = paged_query_result.next_page.value.next_page.value.next_page
          f.value.should be_nil
        end
      end
    end

    describe SynchronousPagedQueryResult do
      let :paged_query_result do
        described_class.new(asynchronous_paged_query_result)
      end

      let :asynchronous_paged_query_result do
        double(:asynchronous_paged_query_result)
      end

      describe '#next_page' do
        let :next_query_result do
          double(:next_query_result)
        end

        it 'delegates to the wrapped query result and wraps the result in an instance of itself' do
          next_query_result.stub(:next_page).and_return(Future.resolved(next_query_result))
          asynchronous_paged_query_result.stub(:next_page).and_return(Future.resolved(next_query_result))
          second_page = paged_query_result.next_page
          second_page.next_page
          next_query_result.should have_received(:next_page)
        end

        it 'returns nil when it is the last page' do
          asynchronous_paged_query_result.stub(:next_page).and_return(Future.resolved(nil))
          paged_query_result.next_page.should be_nil
        end
      end

      describe '#last_page?' do
        it 'delegates to the wrapped query result' do
          asynchronous_paged_query_result.stub(:last_page?).and_return(true)
          paged_query_result.should be_last_page
        end
      end

      describe '#async' do
        it 'returns the asynchronous results' do
          paged_query_result.async.should equal(asynchronous_paged_query_result)
        end
      end
    end
  end
end