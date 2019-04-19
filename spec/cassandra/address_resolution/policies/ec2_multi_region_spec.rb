# encoding: utf-8

#--
# Copyright DataStax, Inc.
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
  module AddressResolution
    module Policies
      describe(EC2MultiRegion) do
        let(:resolver) { double('DNS resolver') }
        subject { EC2MultiRegion.new(resolver) }

        describe('#resolve') do
          let(:address)      { ::IPAddr.new('23.21.218.233') }
          let(:revesed_name) { ::Resolv::DNS::Name.create('233.218.21.23.in-addr.arpa') }
          let(:hostname)     { 'ec2-23-21-218-233.compute-1.amazonaws.com' }
          let(:resolved_ip)  { '10.10.24.1' }

          it 'performs reverse DNS lookup of the hostname' do
            expect(resolver).to receive(:each_name).once.with(revesed_name).and_yield(hostname)
            expect(resolver).to receive(:each_address).with(hostname).and_yield(resolved_ip)

            expect(subject.resolve(address)).to eq(resolved_ip)
          end
        end
      end
    end
  end
end
