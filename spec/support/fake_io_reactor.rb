# encoding: utf-8

class FakeIoReactor
  attr_reader :connections, :last_used_connection

  def initialize
    @running = false
    @connections = []
    @queued_responses = Hash.new { |h, k| h[k] = [] }
    @default_host = nil
    @connection_listeners = []
    @started_promise = Cql::Promise.new
    @before_startup_handler = nil
    @down_nodes = []
  end

  def node_down(hostname)
    @down_nodes << hostname
  end

  def node_up(hostname)
    @down_nodes.delete(hostname)
  end

  def before_startup(&handler)
    @before_startup_handler = handler
  end

  def connect(host, port, timeout)
    if host == '0.0.0.0'
      Cql::Future.failed(Cql::Io::ConnectionError.new('Can\'t connect to 0.0.0.0'))
    elsif @down_nodes.include?(host)
      Cql::Future.failed(Cql::Io::ConnectionError.new('Node down'))
    else
      connection = FakeConnection.new(host, port, timeout)
      @connections << connection
      @connection_listeners.each do |listener|
        listener.call(connection)
      end
      Cql::Future.resolved(connection)
    end
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
        @started_promise.fulfill(self)
      end
    elsif !@started_promise.future.completed?
      @started_promise.fulfill(self)
    end
    @started_promise.future
  end

  def stop
    @running = false
    @connections.each(&:close)
    Cql::Future.resolved
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
    @data = {}
    @registered_event_types = []
    @event_listeners = []
    @closed_listeners = []
    @request_handler = method(:default_request_handler)
  end

  def [](key)
    @data[key]
  end

  def []=(key, value)
    @data[key] = value
  end

  def connected?
    !@closed
  end

  def close(cause=nil)
    @closed = true
    @closed_listeners.each { |listener| listener.call(cause) }
  end

  def handle_request(&handler)
    @request_handler = handler
  end

  def on_closed(&listener)
    @closed_listeners << listener
  end

  def on_event(&listener)
    @event_listeners << listener
  end

  def trigger_event(response)
    if @event_listeners.any? && @registered_event_types.include?(response.type)
      @event_listeners.each { |l| l.call(response) }
    end
  end

  def has_event_listener?
    @event_listeners.any? && @registered_event_types.any?
  end

  def send_request(request, timeout=nil)
    if @closed
      Cql::Future.failed(Cql::NotConnectedError.new)
    else
      @requests << request
      case request
      when Cql::Protocol::RegisterRequest
        @registered_event_types.concat(request.events)
      end
      response = @request_handler.call(request, timeout)
      if response.is_a?(Cql::Protocol::SetKeyspaceResultResponse)
        @keyspace = response.keyspace
      end
      Cql::Future.resolved(response)
    end
  end

  def default_request_handler(request, timeout=nil)
    response = @responses.shift
    unless response
      case request
      when Cql::Protocol::StartupRequest
        Cql::Protocol::ReadyResponse.new
      when Cql::Protocol::QueryRequest
        Cql::Protocol::RowsResultResponse.new([], [], nil, nil)
      end
    end
  end
end