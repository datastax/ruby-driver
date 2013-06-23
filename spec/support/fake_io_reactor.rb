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
    @startup_delay = 0
  end

  def startup_delay=(n)
    @startup_delay = n
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
    Thread.start do
      sleep(@startup_delay)
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
  attr_reader :host, :port, :timeout, :requests

  def initialize(host, port, timeout)
    @host = host
    @port = port
    @timeout = timeout
    @requests = []
    @responses = []
    @closed = false
  end

  def close
    @closed = true
  end

  def queue_response(response)
    @responses << response
  end

  def send_request(request)
    if @closed
      Cql::Future.failed(Cql::Io::NotConnectedError.new)
    else
      @requests << request
      Cql::Future.completed(@responses.shift)
    end
  end
end