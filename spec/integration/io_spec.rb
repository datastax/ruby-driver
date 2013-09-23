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
      io_reactor.connect(ENV['CASSANDRA_HOST'], fake_server.port, 1)
      fake_server.await_connects!(1)
    end

    it 'receives data' do
      protocol_handler = io_reactor.connect(ENV['CASSANDRA_HOST'], fake_server.port, 1).value
      fake_server.await_connects!(1)
      fake_server.broadcast!('hello world')
      await { protocol_handler.data.bytesize > 0 }
      protocol_handler.data.should == 'hello world'
    end

    it 'receives data on multiple connections' do
      protocol_handlers = Array.new(10) { io_reactor.connect(ENV['CASSANDRA_HOST'], fake_server.port, 1).value }
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
        io_reactor.connect(ENV['CASSANDRA_HOST'], 6379, 1).value
      rescue Cql::Io::ConnectionError
        nil
      end
    end

    before do
      io_reactor.start.value
    end

    after do
      io_reactor.stop.value
    end

    it 'can set a value' do
      pending('Redis not running', unless: protocol_handler)
      response = protocol_handler.send_request('SET', 'foo', 'bar').value
      response.should == 'OK'
    end

    it 'can get a value' do
      pending('Redis not running', unless: protocol_handler)
      f = protocol_handler.send_request('SET', 'foo', 'bar').flat_map do
        protocol_handler.send_request('GET', 'foo')
      end
      f.value.should == 'bar'
    end

    it 'can delete values' do
      pending('Redis not running', unless: protocol_handler)
      f = protocol_handler.send_request('SET', 'hello', 'world').flat_map do
        protocol_handler.send_request('DEL', 'hello')
      end
      f.value.should == 1
    end

    it 'handles nil values' do
      pending('Redis not running', unless: protocol_handler)
      f = protocol_handler.send_request('DEL', 'hello').flat_map do
        protocol_handler.send_request('GET', 'hello')
      end
      f.value.should be_nil
    end

    it 'handles errors' do
      pending('Redis not running', unless: protocol_handler)
      f = protocol_handler.send_request('SET', 'foo')
      expect { f.value }.to raise_error("ERR wrong number of arguments for 'set' command")
    end

    it 'handles replies with multiple elements' do
      pending('Redis not running', unless: protocol_handler)
      f = protocol_handler.send_request('DEL', 'stuff')
      f.value
      f = protocol_handler.send_request('RPUSH', 'stuff', 'hello', 'world')
      f.value.should == 2
      f = protocol_handler.send_request('LRANGE', 'stuff', 0, 2)
      f.value.should == ['hello', 'world']
    end

    it 'handles nil values when reading multiple elements' do
      pending('Redis not running', unless: protocol_handler)
      protocol_handler.send_request('DEL', 'things')
      protocol_handler.send_request('HSET', 'things', 'hello', 'world')
      f = protocol_handler.send_request('HMGET', 'things', 'hello', 'foo')
      f.value.should == ['world', nil]
    end
  end
end

module IoSpec
  class TestConnection
    def initialize(connection, scheduler)
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

  class LineProtocolHandler
    def initialize(connection, scheduler)
      @connection = connection
      @connection.on_data(&method(:process_data))
      @lock = Mutex.new
      @buffer = ''
      @requests = []
    end

    def on_line(&listener)
      @line_listener = listener
    end

    def write(command_string)
      @connection.write(command_string)
    end

    def process_data(new_data)
      lines = []
      @lock.synchronize do
        @buffer << new_data
        while newline_index = @buffer.index("\r\n")
          line = @buffer.slice!(0, newline_index + 2)
          line.chomp!
          lines << line
        end
      end
      lines.each do |line|
        @line_listener.call(line) if @line_listener
      end
    end
  end

  class RedisProtocolHandler
    def initialize(connection, scheduler)
      @line_protocol = LineProtocolHandler.new(connection, scheduler)
      @line_protocol.on_line(&method(:handle_line))
      @lock = Mutex.new
      @responses = []
      @state = BaseState.new(method(:handle_response))
    end

    def send_request(*args)
      promise = Cql::Promise.new
      @lock.synchronize do
        @responses << promise
      end
      request = "*#{args.size}\r\n"
      args.each do |arg|
        arg_str = arg.to_s
        request << "$#{arg_str.bytesize}\r\n#{arg_str}\r\n"
      end
      @line_protocol.write(request)
      promise.future
    end

    def handle_response(result, error=false)
      promise = @lock.synchronize do
        @responses.shift
      end
      if error
        promise.fail(StandardError.new(result))
      else
        promise.fulfill(result)
      end
    end

    def handle_line(line)
      @state = @state.handle_line(line)
    end

    class State
      def initialize(result_handler)
        @result_handler = result_handler
      end

      def complete!(result)
        @result_handler.call(result)
      end

      def fail!(message)
        @result_handler.call(message, true)
      end
    end

    class BulkState < State
      def handle_line(line)
        complete!(line)
        BaseState.new(@result_handler)
      end
    end

    class MultiBulkState < State
      def initialize(result_handler, expected_elements)
        super(result_handler)
        @expected_elements = expected_elements
        @elements = []
      end

      def handle_line(line)
        if line.start_with?('$')
          line.slice!(0, 1)
          if line.to_i == -1
            @elements << nil
          end
        else
          @elements << line
        end
        if @elements.size == @expected_elements
          complete!(@elements)
          BaseState.new(@result_handler)
        else
          self
        end
      end
    end

    class BaseState < State
      def handle_line(line)
        next_state = self
        first_char = line.slice!(0, 1)
        case first_char
        when '+' then complete!(line)
        when ':' then complete!(line.to_i)
        when '-' then fail!(line)
        when '$'
          if line.to_i == -1
            complete!(nil)
          else
            next_state = BulkState.new(@result_handler)
          end
        when '*'
          next_state = MultiBulkState.new(@result_handler, line.to_i)
        end
        next_state
      end
    end
  end
end