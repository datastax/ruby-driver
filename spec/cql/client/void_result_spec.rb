# encoding: utf-8

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

require 'spec_helper'


module Cql
  module Client
    describe VoidResult do
      describe '#empty?' do
        it 'is true' do
          described_class.new.should be_empty
        end
      end

      describe '#last_page?' do
        it 'is true' do
          described_class.new.should be_last_page
        end
      end

      describe '#next_page' do
        it 'returns nil' do
          described_class.new.next_page.should be_nil
        end
      end

      describe '#trace_id' do
        it 'is nil' do
          described_class.new.trace_id.should be_nil
        end

        it 'is set through the constructor' do
          uuid = Uuid.new('63a26b40-3f02-11e3-9531-fb72eff05fbb')
          described_class.new(uuid).trace_id.should == uuid
        end
      end

      describe '#metadata' do
        it 'is empty' do
          described_class.new.metadata.each.to_a.should be_empty
        end
      end

      describe '#each' do
        it 'returns an enumerable' do
          described_class.new.each.should be_a(Enumerable)
        end

        it 'enumerates nothing' do
          described_class.new.each.to_a.should be_empty
        end
      end
    end
  end
end
