# encoding: ascii-8bit

# Copyright 2013-2014 DataStax, Inc.
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


module Cassandra
  module Protocol
    describe RowsResultResponse do
      describe '#void?' do
        it 'is not void' do
          response = RowsResultResponse.new([{'col' => 'foo'}], [['ks', 'tbl', 'col', :varchar]], nil, nil)
          response.should_not be_void
        end
      end

      describe '#to_s' do
        it 'returns a string with metadata and rows' do
          response = RowsResultResponse.new([{'col' => 'foo'}], [['ks', 'tbl', 'col', :varchar]], nil, nil)
          response.to_s.should == 'RESULT ROWS [["ks", "tbl", "col", :varchar]] [{"col"=>"foo"}]'
        end
      end
    end
  end
end
