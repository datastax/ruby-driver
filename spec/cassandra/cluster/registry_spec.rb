# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
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
  class Cluster
    describe(Registry) do
      let(:logger) { double('logger').as_null_object }

      subject { Registry.new(logger) }

      describe('#host_found') do
        let(:listener) { double('listener') }

        before do
          subject.add_listener(listener)
        end

        context('when host is unknown') do
          it 'notifies listeners' do
            expect(listener).to receive(:host_found).twice do |host|
              expect(host.tokens).to eq([])
              expect(host).to be_up

              if host.ip == ::IPAddr.new('127.0.0.1')
                expect(host.broadcast_address).to be_nil
                expect(host.listen_address).to be_nil
              else
                expect(host.broadcast_address).to eq(::IPAddr.new('127.0.0.127'))
                expect(host.listen_address).to eq(::IPAddr.new('127.0.0.128'))
              end
            end

            expect(listener).to receive(:host_up).twice do |host|
              expect(host.tokens).to eq([])
              expect(host).to be_up

              if host.ip == ::IPAddr.new('127.0.0.1')
                expect(host.broadcast_address).to be_nil
                expect(host.listen_address).to be_nil
              else
                expect(host.broadcast_address).to eq(::IPAddr.new('127.0.0.127'))
                expect(host.listen_address).to eq(::IPAddr.new('127.0.0.128'))
              end
            end

            subject.host_found(::IPAddr.new('127.0.0.1'), {})
            subject.host_found(::IPAddr.new('127.0.0.2'), {
                'broadcast_address' => '127.0.0.127',
                'listen_address' => '127.0.0.128'
            })
          end
        end

        context('when host is known') do
          before do
            allow(listener).to receive(:host_found)
            allow(listener).to receive(:host_up)
            subject.host_found(::IPAddr.new('127.0.0.2'), {
                'broadcast_address' => '127.0.0.127',
                'listen_address' => '127.0.0.128'
            })
          end
          let (:host) {
            subject.host('127.0.0.2')
          }

          it 'notifies listeners when doing down to up' do
            # First bring down the host; we want to test that bringing it up doesn't lose
            # info.

            host.instance_variable_set(:@status, :down)

            # Now bring it up and verify that it still has broadcast_address and
            # listen_address.
            expect(listener).to receive(:host_up).once do |host|
              expect(host.tokens).to eq([])
              expect(host).to be_up

              expect(host.broadcast_address).to eq(::IPAddr.new('127.0.0.127'))
              expect(host.listen_address).to eq(::IPAddr.new('127.0.0.128'))
            end

            subject.host_found(::IPAddr.new('127.0.0.2'), {
                'broadcast_address' => '127.0.0.127',
                'listen_address' => '127.0.0.128'
            })
          end

          it 'notifies listeners when doing up to down' do
            expect(listener).to receive(:host_down).once do |host|
              expect(host.tokens).to eq([])
              expect(host).to be_down

              expect(host.broadcast_address).to eq(::IPAddr.new('127.0.0.127'))
              expect(host.listen_address).to eq(::IPAddr.new('127.0.0.128'))
            end

            subject.host_down(::IPAddr.new('127.0.0.2'))
          end

          it 're-creates host when metadata has changed' do
            expect(listener).to receive(:host_down).once do |host|
              expect(host.tokens).to eq([])
              expect(host).to be_down
              expect(host.broadcast_address).to eq(::IPAddr.new('127.0.0.127'))
              expect(host.listen_address).to eq(::IPAddr.new('127.0.0.128'))
            end

            expect(listener).to receive(:host_lost).once do |host|
              expect(host.tokens).to eq([])
              expect(host).to be_down
              expect(host.broadcast_address).to eq(::IPAddr.new('127.0.0.127'))
              expect(host.listen_address).to eq(::IPAddr.new('127.0.0.128'))
            end

            expect(listener).to receive(:host_found).once do |host|
              expect(host.tokens).to eq([])
              expect(host).to be_up
              expect(host.broadcast_address).to eq(::IPAddr.new('127.0.0.227'))
              expect(host.listen_address).to eq(::IPAddr.new('127.0.0.228'))
            end

            expect(listener).to receive(:host_up).once do |host|
              expect(host.tokens).to eq([])
              expect(host).to be_up
              expect(host.broadcast_address).to eq(::IPAddr.new('127.0.0.227'))
              expect(host.listen_address).to eq(::IPAddr.new('127.0.0.228'))
            end

            subject.host_found(::IPAddr.new('127.0.0.2'), {
                'broadcast_address' => '127.0.0.227',
                'listen_address' => '127.0.0.228'
            })
          end
        end
      end
    end
  end
end
