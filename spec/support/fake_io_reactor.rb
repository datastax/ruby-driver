# encoding: utf-8

class FakeIoReactor
  attr_reader :connections, :last_used_connection

  def initialize
    @running = false
    @connections = []
    @queued_responses = Hash.new { |h, k| h[k] = [] }
    @default_host = nil
    @connection_listeners = []
    @started_future = Cql::Future.new
    @before_startup_handler = nil
  end

  def before_startup(&handler)
    @before_startup_handler = handler
  end

  def connect(host, port, timeout)
    connection = FakeConnection.new(host, port, timeout)
    @connections << connection
    @connection_listeners.each do |listener|
      listener.call(connection)
    end
    Cql::Future.completed(connection)
  end

  def on_connection(&listener)
    @connection_listeners << listener
  end

  def start
    @running = true
    if @before_startup_handler
      @before_startup_handler = nil
      Thread.start do
        @before_startup_handler.call
        @started_future.complete!(self)
      end
    elsif !(@started_future.complete? || @started_future.failed?)
      @started_future.complete!(self)
    end
    @started_future
  end

  def stop
    @running = false
    @connections.each(&:close)
    Cql::Future.completed
  end

  def running?
    @running
  end
end

class FakeConnection
  attr_reader :host, :port, :timeout, :requests, :keyspace

  def initialize(host, port, timeout)
    @host = host
    @port = port
    @timeout = timeout
    @requests = []
    @responses = []
    @closed = false
    @keyspace = nil
  end

  def close
    @closed = true
  end

  def queue_response(response)
    @responses << response
  end

  def send_request(request)
    if @closed
      Cql::Future.failed(Cql::NotConnectedError.new)
    else
      @requests << request
      response = @responses.shift
      if response.is_a?(Cql::Protocol::SetKeyspaceResultResponse)
        @keyspace = response.keyspace
      end
      Cql::Future.completed(response)
    end
  end
end