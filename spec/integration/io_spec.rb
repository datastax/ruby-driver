# encoding: utf-8

require 'spec_helper'


describe 'An IO reactor' do
  context 'with a generic server' do
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
      protocol_handler = io_reactor.connect('127.0.0.1', fake_server.port, 1).get
      fake_server.await_connects!(1)
      fake_server.broadcast!('hello world')
      await { protocol_handler.data.bytesize > 0 }
      protocol_handler.data.should == 'hello world'
    end

    it 'receives data on multiple connections' do
      protocol_handlers = Array.new(10) { io_reactor.connect('127.0.0.1', fake_server.port, 1).get }
      fake_server.await_connects!(10)
      fake_server.broadcast!('hello world')
      await { protocol_handlers.all? { |c| c.data.bytesize > 0 } }
      protocol_handlers.sample.data.should == 'hello world'
    end
  end

  context 'when talking to Redis' do
    let :io_reactor do
      Cql::Io::IoReactor.new(IoSpec::RedisProtocolHandler)
    end

    let :protocol_handler do
      begin
        io_reactor.connect('127.0.0.1', 6379, 1).get
      rescue Cql::Io::ConnectionError
        nil
      end
    end

    before do
      io_reactor.start.get
    end

    after do
      io_reactor.stop.get
    end

    it 'can set a value' do
      pending('Redis not running', unless: protocol_handler)
      response = protocol_handler.send_request('SET', 'foo', 'bar').get
      response.should == 'OK'
    end

    it 'can get a value' do
      pending('Redis not running', unless: protocol_handler)
      f = protocol_handler.send_request('SET', 'foo', 'bar').flat_map do
        protocol_handler.send_request('GET', 'foo')
      end
      f.get.should == 'bar'
    end

    it 'can delete values' do
      pending('Redis not running', unless: protocol_handler)
      f = protocol_handler.send_request('SET', 'hello', 'world').flat_map do
        protocol_handler.send_request('DEL', 'hello')
      end
      f.get.should == 1
    end

    it 'handles nil values' do
      pending('Redis not running', unless: protocol_handler)
      f = protocol_handler.send_request('DEL', 'hello').flat_map do
        protocol_handler.send_request('GET', 'hello')
      end
      f.get.should be_nil
    end

    it 'handles errors' do
      pending('Redis not running', unless: protocol_handler)
      f = protocol_handler.send_request('SET', 'foo')
      expect { f.get }.to raise_error("ERR wrong number of arguments for 'set' command")
    end

    it 'handles replies with multiple elements' do
      pending('Redis not running', unless: protocol_handler)
      f = protocol_handler.send_request('DEL', 'stuff')
      f.get
      f = protocol_handler.send_request('RPUSH', 'stuff', 'hello', 'world')
      f.get.should == 2
      f = protocol_handler.send_request('LRANGE', 'stuff', 0, 2)
      f.get.should == ['hello', 'world']
    end

    it 'handles nil values when reading multiple elements' do
      pending('Redis not running', unless: protocol_handler)
      protocol_handler.send_request('DEL', 'things')
      protocol_handler.send_request('HSET', 'things', 'hello', 'world')
      f = protocol_handler.send_request('HMGET', 'things', 'hello', 'foo')
      f.get.should == ['world', nil]
    end
  end
end

module IoSpec
  class TestConnection
    def initialize(connection)
      @connection = connection
      @connection.on_data(&method(:receive_data))
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

  class RedisProtocolHandler
    def initialize(connection)
      @connection = connection
      @connection.on_data(&method(:receive_data))
      @lock = Mutex.new
      @buffer = Cql::ByteBuffer.new
      @responses = []
    end

    def send_request(*args)
      future = Cql::Future.new
      @lock.synchronize do
        @responses << future
      end
      request = "*#{args.size}\r\n"
      args.each do |arg|
        arg_str = arg.to_s
        request << "$#{arg_str.bytesize}\r\n#{arg_str}\r\n"
      end
      @connection.write(request)
      future
    end

    def receive_data(new_data)
      @lock.synchronize do
        @buffer << new_data
      end
      deliver_responses
    end

    def deliver_responses
      while (value = read_line_or_value)
        if value.start_with?('+')
          @responses.shift.complete!(value[1, value.bytesize - 1])
        elsif value.start_with?('$')
          @next_size = value[1, value.bytesize - 1].to_i
          if @next_size == -1
            if @state == :multi_bulk
              @args << nil
              @arg_count -= 1
            else
              @responses.shift.complete!(nil)
            end
            @next_size = nil
          elsif @state.nil?
            @state = :bulk
          end
        elsif value.start_with?(':')
          n = value[1, value.bytesize - 1].to_i
          @responses.shift.complete!(n)
        elsif value.start_with?('-')
          message = value[1, value.bytesize - 1]
          @responses.shift.fail!(StandardError.new(message))
        elsif value.start_with?('*')
          @arg_count = value[1, value.bytesize - 1].to_i
          @args = []
          @state = :multi_bulk
        else
          case @state
          when :bulk
            @responses.shift.complete!(value)
            @state = nil
          when :multi_bulk
            @args << value
            @arg_count -= 1
          end
        end
        if @arg_count == 0
          @responses.shift.complete!(@args)
          @args = nil
          @arg_count = nil
          @state = nil
        end
      end
    end

    def read_line_or_value
      @lock.synchronize do
        if @next_size
          if @buffer.bytesize >= @next_size + 2
            bytes = @buffer.read(@next_size)
            @buffer.discard(2)
            @next_size = nil
            bytes
          end
        elsif (index = @buffer.cheap_peek.index("\r\n") || @buffer.to_s.index("\r\n"))
          line = @buffer.read(index)
          @buffer.discard(2)
          line
        end
      end
    end
  end
end