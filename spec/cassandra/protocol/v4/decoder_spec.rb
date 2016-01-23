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

module Cassandra
  module Protocol
    module V4
      describe(Decoder) do
        let(:handler) { double('handler') }
        let(:subject) { Decoder.new(handler) }

        describe('#<<') do
          context('with incomplete UDTs') do
            let(:data) { "\x84\x00\x00\x00\b\x00\x00\x00\xAE\x00\x00\x00\x02" \
                         "\x00\x00\x00\x01\x00\x00\x00\x02\x00\asimplex\x00" \
                         "\x05users\x00\x02id\x00\t\x00\blocation\x00 \x000" \
                         "\x00\asimplex\x00\aaddress\x00\x03\x00\x06street" \
                         "\x00\r\x00\azipcode\x00\t\x00\x04city\x00\r\x00" \
                         "\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x00\x00" \
                         "\x00\x00=\x00\x00\x00\x02\x00\x00\x00\x18\x00\x00" \
                         "\x00\f123 Main St.\x00\x00\x00\x04\x00\x013\x83" \
                         "\x00\x00\x00\x19\x00\x00\x00\r4567 Main St.\x00\x00" \
                         "\x00\x04\x00\x01Vf" }

            it('treats missing values as nil') do
              expect(handler).to receive(:complete_request) do |id, response|
                expect(id).to eq(0)
                expect(response.metadata).to eq([
                  ['simplex', 'users', 'id', Types.int],
                  ['simplex', 'users', 'location', Types.list(
                      Types.udt('simplex', 'address',
                                'street', Types.text,
                                'zipcode', Types.int,
                                'city', Types.text))]
                ])
                expect(response.rows.first).to eq({
                  'id' => 0,
                  'location' => [
                    Cassandra::UDT.new('street', '123 Main St.', 'zipcode', 78723,
                                       'city', nil),
                    Cassandra::UDT.new('street', '4567 Main St.', 'zipcode', 87654,
                                       'city', nil)
                  ]
                })
              end
              subject << data
            end
          end

          context('with compressed body') do
            let(:subject) {
              Decoder.new(handler, Cassandra::Compression::Compressors::Snappy.new)
            }

            it 'should decode properly' do
              expect(handler).to receive(:complete_request) do |id, response|
                expect(id).to eq(0)
                expect(response.rows.count).to eq(2)
              end
              subject << "\x84\x01\x00\x00\b\x00\x00\x00<K\x1C\x00\x00\x00\x02\x00\x00\x00\x01\x05\bd\asimplex\x00\x04base\x00\x02f1\x00\t\x00\x02f2\x00\t\x05\x1F\b\x00\x00\x04\r+.\b\x00 \x02\x00\x00\x00\x04\x00\x00\x00\x02"
            end

            it 'should handle extra cruft in compressed body' do
              expect(handler).to receive(:complete_request) do |id, response|
                expect(id).to eq(0)
                expect(response.rows.count).to eq(2)
              end
              subject << "\x84\x01\x00\x00\b\x00\x00\x00@O\x1C\x00\x00\x00\x02\x00\x00\x00\x01\x05\bd\asimplex\x00\x04base\x00\x02f1\x00\t\x00\x02f2\x00\t\x05\x1F\b\x00\x00\x04\r+.\b\x000\x02\x00\x00\x00\x04\x00\x00\x00\x02JUNK"
            end
          end

          context('with multiple frames') do
            it 'should handle frames containing warnings' do
              expect(handler).to receive(:complete_request) do |id, response|
                expect(id).to eq(0)
                expect(response.warnings).
                    to eq(['Unlogged batch covering 2 partitions detected against table [simplex.base]. You should use a logged batch for atomicity, or asynchronous writes for performance.'])
              end
              expect(handler).to receive(:complete_request) do |id, response|
                expect(id).to eq(1)
                expect(response.warnings).
                    to eq(['Unlogged batch covering 2 partitions detected against table [simplex.base]. You should use a logged batch for atomicity, or asynchronous writes for performance.'])
              end
              subject << "\x84\b\x00\x00\b\x00\x00\x00\xA8\x00\x01\x00\xA0Unlogged batch covering 2 partitions detected against table [simplex.base]. You should use a logged batch for atomicity, or asynchronous writes for performance.\x00\x00\x00\x01" \
                         "\x84\b\x00\x01\b\x00\x00\x00\xA8\x00\x01\x00\xA0Unlogged batch covering 2 partitions detected against table [simplex.base]. You should use a logged batch for atomicity, or asynchronous writes for performance.\x00\x00\x00\x01"
            end

            it 'should handle frames containing trace data' do
              expect(handler).to receive(:complete_request) do |id, response|
                expect(id).to eq(0)
                expect(response.trace_id).
                    to eq(Uuid.new("d5399070-c142-11e5-a48b-09d3de2f1e32"))
              end
              expect(handler).to receive(:complete_request) do |id, response|
                expect(id).to eq(1)
                expect(response.trace_id).
                    to eq(Uuid.new("d539de92-c142-11e5-a48b-09d3de2f1e32"))
              end

              subject << "\x84\x02\x00\x00\b\x00\x00\x00\x14\xD59\x90p\xC1B\x11\xE5\xA4\x8B\t\xD3\xDE/\x1E2\x00\x00\x00\x01" \
                         "\x84\x02\x00\x01\b\x00\x00\x00\x14\xD59\xDE\x92\xC1B\x11\xE5\xA4\x8B\t\xD3\xDE/\x1E2\x00\x00\x00\x01"
            end

            it 'should handle cruft in frames' do
              # This is basically the same test as the warnings test above, but the first
              # frame has a few more characters in the body, which should be ignored.
              expect(handler).to receive(:complete_request) do |id, response|
                expect(id).to eq(0)
                expect(response.warnings).
                    to eq(['Unlogged batch covering 2 partitions detected against table [simplex.base]. You should use a logged batch for atomicity, or asynchronous writes for performance.'])
              end
              expect(handler).to receive(:complete_request) do |id, response|
                expect(id).to eq(1)
                expect(response.warnings).
                    to eq(['Unlogged batch covering 2 partitions detected against table [simplex.base]. You should use a logged batch for atomicity, or asynchronous writes for performance.'])
              end
              subject << "\x84\b\x00\x00\b\x00\x00\x00\xAC\x00\x01\x00\xA0Unlogged batch covering 2 partitions detected against table [simplex.base]. You should use a logged batch for atomicity, or asynchronous writes for performance.\x00\x00\x00\x01JUNK" \
                         "\x84\b\x00\x01\b\x00\x00\x00\xA8\x00\x01\x00\xA0Unlogged batch covering 2 partitions detected against table [simplex.base]. You should use a logged batch for atomicity, or asynchronous writes for performance.\x00\x00\x00\x01"
            end
          end
        end
      end
    end
  end
end
