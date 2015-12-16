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
            let(:data) { "\x83\x00\x00\x00\b\x00\x00\x00\xAE\x00\x00\x00\x02" \
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
                  ['simplex', 'users', 'location', Types.list(Types.udt('simplex', 'address',
                                                                        'street', Types.text,
                                                                        'zipcode', Types.int,
                                                                        'city', Types.text))]
                ])
                expect(response.rows.first).to eq({
                  'id' => 0,
                  'location' => [
                    Cassandra::UDT.new('street', '123 Main St.', 'zipcode', 78723, 'city', nil),
                    Cassandra::UDT.new('street', '4567 Main St.', 'zipcode', 87654, 'city', nil)
                  ]
                })
              end
              subject << data
            end
          end
        end
      end
    end
  end
end
