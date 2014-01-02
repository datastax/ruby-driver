# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe QueryResult do
      let :result do
        described_class.new(metadata, rows, trace_id)
      end

      let :metadata do
        [
          ['ks', 'tbl', 'col1', :varchar],
          ['ks', 'tbl', 'col2', :double],
        ]
      end

      let :rows do
        [double(:row1), double(:row2), double(:row3)]
      end

      let :trace_id do
        double(:trace_id)
      end

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
  end
end