# encoding: utf-8

#--
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
            expect(listener).to receive(:host_found).once do |host|
              expect(host.ip).to eq(IPAddr.new('127.0.0.1'))
              expect(host.tokens).to eq([])
              expect(host).to be_up
            end

            expect(listener).to receive(:host_up).once do |host|
              expect(host.ip).to eq(IPAddr.new('127.0.0.1'))
              expect(host.tokens).to eq([])
              expect(host).to be_up
            end

            subject.host_found(IPAddr.new('127.0.0.1'), {})
          end
        end
      end
    end
  end
end
