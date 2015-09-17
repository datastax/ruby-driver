# encoding: ascii-8bit

#--
# Copyright 2013-2015 DataStax, Inc.
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
require 'yaml'

module Cassandra
  module Protocol
    describe(Coder) do
      describe('.read_timestamp') do
        let(:buffer) { CqlByteBuffer.new }

        it "maintains microsenconds precision" do
          original = Time.now
          decoded  = Coder.read_timestamp(Coder.write_timestamp(buffer, original))

          decoded_int  = (decoded.to_r.to_f * 1000).to_i
          original_int = (original.to_r.to_f * 1000).to_i

          expect(decoded_int).to eq(original_int)
        end
      end

      describe('RUBY-128') do
        it 'reads very large short strings and string' do
          column_specs = ::YAML::load(File.open("cols.yml")) 
          buffer       = ::YAML::load(File.open("buffer.yml")) 

          Coder.read_values_v1(buffer, column_specs)
        end
      end
    end
  end
end
