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
    describe CredentialsRequest do
      describe '#write' do
        let(:encoder) { double('encoder') }

        it 'encodes a CREDENTIALS request frame' do
          bytes = CredentialsRequest.new('username' => 'cassandra', 'password' => 'ardnassac').write(CqlByteBuffer.new, 1, encoder)
          bytes.should eql_bytes(
            "\x00\x02" +
            "\x00\x08username" +
            "\x00\x09cassandra" +
            "\x00\x08password" +
            "\x00\x09ardnassac"
          )
        end
      end

      describe '#to_s' do
        it 'returns a pretty string' do
          request = CredentialsRequest.new('foo' => 'bar', 'hello' => 'world')
          request.to_s.should == 'CREDENTIALS {"foo"=>"bar", "hello"=>"world"}'
        end
      end

      describe '#eql?' do
        it 'returns when the credentials are the same' do
          c1 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
          c2 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
          c2.should eql(c2)
        end

        it 'returns when the credentials are equivalent' do
          pending 'this would be nice, but is hardly necessary'
          c1 = CredentialsRequest.new(:username => 'foo', :password => 'bar')
          c2 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
          c1.should eql(c2)
        end

        it 'returns false when the credentials are different' do
          c1 = CredentialsRequest.new('username' => 'foo', 'password' => 'world')
          c2 = CredentialsRequest.new('username' => 'foo', 'hello' => 'world')
          c1.should_not eql(c2)
        end

        it 'is aliased as ==' do
          c1 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
          c2 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
          c1.should == c2
        end
      end

      describe '#hash' do
        it 'has the same hash code as another identical object' do
          c1 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
          c2 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
          c1.hash.should == c2.hash
        end

        it 'has the same hash code as another object with equivalent credentials' do
          pending 'this would be nice, but is hardly necessary'
          c1 = CredentialsRequest.new(:username => 'foo', :password => 'bar')
          c2 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
          c1.hash.should == c2.hash
        end

        it 'does not have the same hash code when the credentials are different' do
          c1 = CredentialsRequest.new('username' => 'foo', 'password' => 'world')
          c2 = CredentialsRequest.new('username' => 'foo', 'hello' => 'world')
          c1.hash.should_not == c2.hash
        end
      end
    end
  end
end
