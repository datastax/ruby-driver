# encoding: utf-8

# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

class FakeIoReactor
  class Timer
    def initialize(promise, timeout)
      @promise = promise
      @timeout = timeout
    end

    def advance(time)
      @timeout -= time
      @promise.fulfill(object_id) if @timeout <= 0

      self
    end

    def expired?
      @timeout <= 0
    end
  end

  attr_reader :connections, :last_used_connection

  def initialize
    @running = false
    @connections = []
    @queued_responses = Hash.new { |h, k| h[k] = [] }
    @default_host = nil
    @connection_listeners = []
    @started_promise = Ione::Promise.new
    @before_startup_handler = nil
    @down_nodes = []
    @timers = []
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
      Ione::Future.failed(Cassandra::Io::ConnectionError.new('Can\'t connect to 0.0.0.0'))
    elsif @down_nodes.include?(host)
      Ione::Future.failed(Cassandra::Io::ConnectionError.new('Node down'))
    else
      connection = FakeConnection.new(host, port, timeout)
      @connections << connection
      @connection_listeners.each do |listener|
        listener.call(connection)
      end
      Ione::Future.resolved(connection)
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
    Ione::Future.resolved
  end

  def running?
    @running
  end

  def schedule_timer(seconds)
    promise = Ione::Promise.new
    @timers << Timer.new(promise, seconds)
    promise.future
  end

  def advance_time(seconds)
    @timers.dup.each {|timer| timer.advance(seconds)}
    @timers.reject! {|timer| timer.expired?}

    self
  end

  def execute
    Ione::Future.resolved(yield)
  rescue => e
    Ione::Future.failed(e)
  end
end

class FakeConnection
  attr_reader :host, :port, :timeout, :requests, :keyspace

  def initialize(host, port, timeout, data={})
    @host = host
    @port = port
    @timeout = timeout
    @requests = []
    @responses = []
    @closed = false
    @keyspace = nil
    @data = data
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
    Ione::Future.resolved
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
      Ione::Future.failed(Cassandra::Errors::NotConnectedError.new)
    else
      @requests << request
      case request
      when Cassandra::Protocol::RegisterRequest
        @registered_event_types.concat(request.events)
      end
      catch(:halt) do
        response = @request_handler.call(request, timeout)
        if response.is_a?(Cassandra::Protocol::SetKeyspaceResultResponse)
          @keyspace = response.keyspace
        end
        Ione::Future.resolved(response)
      end
    end
  rescue => e
    Ione::Future.failed(e)
  end

  def default_request_handler(request, timeout=nil)
    response = @responses.shift
    unless response
      case request
      when Cassandra::Protocol::StartupRequest
        Cassandra::Protocol::ReadyResponse.new
      when Cassandra::Protocol::QueryRequest
        Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
      end
    end
  end
end