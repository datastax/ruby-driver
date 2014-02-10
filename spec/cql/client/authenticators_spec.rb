# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe PlainTextAuthProvider do
      let :auth_provider do
        described_class.new('foo', 'bar')
      end

      let :standard_authentication_class do
        'org.apache.cassandra.auth.PasswordAuthenticator'
      end

      describe '#create_authenticator' do
        it 'creates a PlainTextAuthenticator for protocol v2' do
          authenticator = auth_provider.create_authenticator(standard_authentication_class, 2)
          authenticator.initial_response.should == "\x00foo\x00bar"
        end

        it 'creates a PlainTextAuthenticator for protocol v2 and above' do
          authenticator = auth_provider.create_authenticator(standard_authentication_class, 5)
          authenticator.initial_response.should == "\x00foo\x00bar"
        end

        it 'creates a CredentialsAuthenticator for protocol v1' do
          authenticator = auth_provider.create_authenticator(standard_authentication_class, 1)
          authenticator.initial_response.should eql('username' => 'foo', 'password' => 'bar')
        end

        it 'returns nil when the authentication class is not o.a.c.a.PasswordAuthenticator' do
          authenticator = auth_provider.create_authenticator('org.acme.Foo', 1)
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
    end

    describe CredentialsAuthenticator do
      describe '#initial_response' do
        it 'returns the credentials' do
          response = described_class.new('username' => 'user', 'password' => 'pass', 'shoe_size' => 34).initial_response
          response.should eql('username' => 'user', 'password' => 'pass', 'shoe_size' => 34)
        end
      end
    end
  end
end