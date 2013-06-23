# encoding: utf-8

require 'spec_helper'


describe 'An IO reactor' do
  let :io_reactor do
    Cql::Io::IoReactor.new(IoSpec::TestConnection)
  end

  let :fake_server do
    FakeServer.new
  end

  before do
    fake_server.start!
    io_reactor.start
  end

  after do
    io_reactor.stop
    fake_server.stop!
  end

  it 'connects to the server' do
    io_reactor.connect('127.0.0.1', fake_server.port, 1)
    fake_server.await_connects!(1)
  end

  it 'receives data' do
    connection = io_reactor.connect('127.0.0.1', fake_server.port, 1).get
    fake_server.await_connects!(1)
    fake_server.broadcast!('hello world')
    await { connection.data.bytesize > 0 }
    connection.data.should == 'hello world'
  end
end

module IoSpec
  class TestConnection
    def initialize(socket_handler)
      @socket_handler = socket_handler
      @socket_handler.on_data(&method(:receive_data))
      @lock = Mutex.new
      @data = Cql::ByteBuffer.new
    end

    def data
      @lock.synchronize { @data.to_s }
    end

    private

    def receive_data(new_data)
      @lock.synchronize { @data << new_data }
    end
  end
end