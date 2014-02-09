# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe PasswordAuthenticator do
      let :authenticator do
        described_class.new('larry', 's3cr3t')
      end

      describe '#supports?' do
        context 'with protocol v1' do
          it 'returns true for Cassandra\'s built in PasswordAuthenticator' do
            authenticator.supports?('org.apache.cassandra.auth.PasswordAuthenticator', 1).should be_true
          end

          it 'returns false for all other inputs' do
            authenticator.supports?('foo.bar.Acme', 1).should be_false
          end
        end

        context 'with protocol v2' do
          it 'returns true with Cassandra\'s built in PasswordAuthenticator' do
            authenticator.supports?('org.apache.cassandra.auth.PasswordAuthenticator', 2).should be_true
          end

          it 'returns false for all other inputs' do
            authenticator.supports?('foo.bar.Acme', 2).should be_false
          end
        end

        context 'with another protocol version' do
          it 'returns false' do
            authenticator.supports?('org.apache.cassandra.auth.PasswordAuthenticator', 9).should be_false
          end
        end
      end

      describe '#initial_request' do
        context 'with protocol v1' do
          it 'returns a CredentialsRequest with the username and password' do
            request = authenticator.initial_request(1)
            request.credentials.should eql(username: 'larry', password: 's3cr3t')
          end
        end

        context 'with protocol v2' do
          it 'returns an AuthResponseRequest' do
            request = authenticator.initial_request(2)
            request.should == Protocol::AuthResponseRequest.new("\x00larry\x00s3cr3t")
          end
        end

        context 'with another protocol version' do
          it 'raises an error' do
            expect { authenticator.initial_request(9) }.to raise_error(UnsupportedProtocolVersionError)
          end
        end
      end
    end
  end
end