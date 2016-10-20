# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

require 'spec_helper'

include Cassandra::Types
module Cassandra
  describe(Keyspace) do
    let(:view) { double('view') }
    let(:table) { double('table') }
    let(:ks) { Keyspace.new('myks', true, nil, {'mytable' => table}, nil, nil, nil, {'myview' => view}) }

    before do
      allow(view).to receive(:set_keyspace)
      allow(table).to receive(:set_keyspace)
    end

    context :has_materialized_view? do
      it 'should return true if the view exists and has a base-table' do
        expect(view).to receive(:base_table).and_return(:table)
        expect(ks.has_materialized_view?('myview')).to be_truthy
      end

      it 'should return false if the view exists but does not have a base-table' do
        expect(view).to receive(:base_table).and_return(nil)
        expect(ks.has_materialized_view?('myview')).to be_falsey
      end
    end

    context :materialized_view do
      it 'should return the view if it exists and has a base-table' do
        expect(view).to receive(:base_table).and_return(:table)
        expect(ks.materialized_view('myview')).to be(view)
      end

      it 'should return nil if the view exists but does not have a base-table' do
        expect(view).to receive(:base_table).and_return(nil)
        expect(ks.materialized_view('myview')).to be_nil
      end
    end

    context :materialized_views do
      it 'should return the view if it exists and has a base-table' do
        expect(view).to receive(:base_table).and_return(:table)
        expect(ks.materialized_views).to eq([view])
      end

      it 'should not return view if the view exists but does not have a base-table' do
        expect(view).to receive(:base_table).and_return(nil)
        expect(ks.materialized_views).to be_empty
      end
    end

    context :each_materialized_view do
      it 'should return the view if it exists and has a base-table' do
        expect(view).to receive(:base_table).and_return(:table)
        result = []
        ks.each_materialized_view do |v|
          result << v
        end

        expect(result).to eq([view])
      end

      it 'should not return view if the view exists but does not have a base-table' do
        expect(view).to receive(:base_table).and_return(nil)
        result = []
        ks.each_materialized_view do |v|
          result << v
        end

        expect(result).to be_empty
      end
    end
  end
end
