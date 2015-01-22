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
    module V1
      describe(Decoder) do
        let(:handler)    { double('handler') }
        let(:decoder)    { Decoder.new(handler, compressor) }

        describe('#<<') do
          let(:protocol_version) { 1 }
          let(:flags)            { 0 }
          let(:stream_id)        { 0 }

          let(:buffer) do
            CqlByteBuffer.new([
              protocol_version,
              flags,
              stream_id,
              opcode,
              body.bytesize
            ].pack('c4N') + body)
          end

          context('without compressor') do
            let(:compressor) { nil }

            context('and auth challenge response') do
              let(:opcode) { 0x0e }

              context('with token') do
                let(:body) { "\x00\x00\x00\x0cbingbongpong" }

                it 'decodes with token' do
                  expect(handler).to receive(:complete_request).with(stream_id, an_instance_of(AuthChallengeResponse)) do |_, response|
                    expect(response.token).to eq('bingbongpong')
                  end

                  decoder << buffer
                end
              end

              context('without token') do
                let(:body) { "\xff\xff\xff\xff" }

                it 'decodes without token' do
                  expect(handler).to receive(:complete_request).with(stream_id, an_instance_of(AuthChallengeResponse)) do |_, response|
                    expect(response.token).to be_nil
                  end

                  decoder << buffer
                end
              end
            end

            context('and auth success response') do
              let(:opcode) { 0x10 }

              context('with token') do
                let(:body) { "\x00\x00\x00\x0cbingbongpong" }

                it 'decodes with token' do
                  expect(handler).to receive(:complete_request).with(stream_id, an_instance_of(AuthSuccessResponse)) do |_, response|
                    expect(response.token).to eq('bingbongpong')
                  end

                  decoder << buffer
                end
              end

              context('without token') do
                let(:body) { "\xff\xff\xff\xff" }

                it 'decodes without token' do
                  expect(handler).to receive(:complete_request).with(stream_id, an_instance_of(AuthSuccessResponse)) do |_, response|
                    expect(response.token).to be_nil
                  end

                  decoder << buffer
                end
              end
            end

            context('and authenticate response') do
              let(:opcode) { 0x03 }
              let(:body)   { "\x00\x2forg.apache.cassandra.auth.PasswordAuthenticator" }

              it 'decodes authentication class' do
                expect(handler).to receive(:complete_request).with(stream_id, an_instance_of(AuthenticateResponse)) do |_, response|
                  expect(response.authentication_class).to eq('org.apache.cassandra.auth.PasswordAuthenticator')
                end

                decoder << buffer
              end
            end
          end

          context('with compressor') do
            let(:compressor) { double('compressor') }

            context('and auth challenge response') do
            end
          end
        end
      end
    end
  end
end
