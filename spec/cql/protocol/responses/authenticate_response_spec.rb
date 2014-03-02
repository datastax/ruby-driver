# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe AuthenticateResponse do
      describe '.decode' do
        let :response do
          buffer = CqlByteBuffer.new("\x00\x2forg.apache.cassandra.auth.PasswordAuthenticator")
          described_class.decode(1, buffer, buffer.length)
        end

        it 'decodes the authentication class' do
          response.authentication_class.should == 'org.apache.cassandra.auth.PasswordAuthenticator'
        end
      end

      describe '#to_s' do
        it 'returns a string with the authentication class' do
          response = described_class.new('org.apache.cassandra.auth.PasswordAuthenticator')
          response.to_s.should == 'AUTHENTICATE org.apache.cassandra.auth.PasswordAuthenticator'
        end
      end
    end
  end
end
