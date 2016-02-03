# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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
  module Auth
    module Providers
      describe Password do
        let :auth_provider do
          described_class.new('foo', 'bar')
        end

        let :standard_authentication_class do
          'org.apache.cassandra.auth.PasswordAuthenticator'
        end

        describe '#create_authenticator' do
          it 'creates a PasswordAuthenticator' do
            authenticator = auth_provider.create_authenticator(standard_authentication_class)
            authenticator.initial_response.should == "\x00foo\x00bar"
          end

          it 'returns nil when the authentication class is not org.apache.cassandra.auth.PasswordAuthenticator' do
            authenticator = auth_provider.create_authenticator('org.acme.Foo')
            authenticator.should be_nil
          end
        end
      end

      describe Password::Authenticator do
        describe '#initial_response' do
          it 'encodes the username and password' do
            response = described_class.new('user', 'pass').initial_response
            response.should == "\x00user\x00pass"
          end
        end

        describe '#challenge_response' do
          it 'returns nil' do
            authenticator = described_class.new('user', 'pass')
            authenticator.initial_response
            authenticator.challenge_response('?').should be_nil
          end
        end

        describe '#authentication_successful' do
          it 'does nothing' do
            authenticator = described_class.new('user', 'pass')
            authenticator.initial_response
            authenticator.challenge_response('?')
            authenticator.authentication_successful('ok')
          end
        end
      end
    end
  end
end
