# encoding: utf-8

require 'spec_helper'

module Cql
  describe(Builder) do
    let :builder do
      Builder.new
    end

    describe '#build' do
      it 'creates a new Driver and connects it to 127.0.0.1' do
        driver = double('driver')
        expect(Driver).to receive(:new).once.with({}).and_return(driver)
        expect(driver).to receive(:connect).once.with(::Set[IPAddr.new('127.0.0.1')]).and_return(driver)
        expect(driver).to receive(:get).once.and_return(driver)

        expect(builder.build).to eq(driver)
      end

      context 'with_contact_points("127.0.0.1", "127.0.0.2", "127.0.0.3")' do
        it 'creates a new Driver and connects it to [127.0.0.1, 127.0.0.2, 127.0.0.3]' do
          driver = double('driver').as_null_object
          Driver.stub(:new) { driver }
          expect(driver).to receive(:connect).once.with(::Set[
                              IPAddr.new('127.0.0.1'),
                              IPAddr.new('127.0.0.2'),
                              IPAddr.new('127.0.0.3')
                            ]).and_return(driver)

          builder
            .with_contact_points('127.0.0.1', '127.0.0.2', '127.0.0.3')
            .build
        end
      end

      context 'with_logger' do
        let(:logger) { double('logger') }
        let(:driver) { double('driver').as_null_object }

        it 'passes a default logger to the driver' do
          expect(Driver).to receive(:new).once.with(:logger => logger).and_return(driver)

          builder
            .with_logger(logger)
            .build
        end
      end

      context 'with_credentials' do
        let(:username) { 'username' }
        let(:password) { 'password' }
        let(:auth_provider) { double('auth provider') }
        let(:driver) { double('driver').as_null_object }

        it 'passes credentials and auth_provider to the driver' do
          expect(Auth::PlainTextAuthProvider).to receive(:new).with(username, password).and_return(auth_provider)
          expect(Driver).to receive(:new).once.with({
                              :credentials => {
                                :username => username,
                                :password => password
                              },
                              :auth_provider => auth_provider
                            }).and_return(driver)

          builder.with_credentials(username, password).build
        end
      end

      context 'with_compresion' do
        let(:driver)     { double('driver').as_null_object }

        it 'passes compressor to the driver' do
          expect(Driver).to receive(:new).once.with(:compressor => kind_of(Compression::Lz4Compressor)).and_return(driver)
          builder.with_compresion(:lz4).build
        end
      end

      context 'with_port' do
        let(:driver) { double('driver').as_null_object }

        it 'passes port to the driver' do
          expect(Driver).to receive(:new).once.with(:port => 123).and_return(driver)
          builder.with_port(123).build
        end
      end
    end
  end
end
