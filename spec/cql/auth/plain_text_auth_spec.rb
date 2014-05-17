# encoding: utf-8

require 'spec_helper'


module Cql
  module Auth
    describe PlainTextAuthProvider do
      let :auth_provider do
        described_class.new('foo', 'bar')
      end

      let :standard_authentication_class do
        'org.apache.cassandra.auth.PasswordAuthenticator'
      end

      describe '#create_authenticator' do
        it 'creates a PlainTextAuthenticator' do
          authenticator = auth_provider.create_authenticator(standard_authentication_class)
          authenticator.initial_response.should == "\x00foo\x00bar"
        end

        it 'returns nil when the authentication class is not o.a.c.a.PasswordAuthenticator' do
          authenticator = auth_provider.create_authenticator('org.acme.Foo')
          authenticator.should be_nil
        end
      end
    end

    describe PlainTextAuthenticator do
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