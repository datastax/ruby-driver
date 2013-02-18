# encoding: utf-8

class FakeIoReactor
  attr_reader :connections, :last_used_connection

  def initialize
    @running = false
    @connections = []
    @queued_responses = Hash.new { |h, k| h[k] = [] }
    @default_host = nil
  end

  def queue_response(response, host=nil)
    @queued_responses[host] << response
  end

  def start
    @running = true
    @connections.each do |connection|
      connection[:future].complete! unless connection[:future].complete?
    end
    Cql::Future.completed
  end

  def stop
    @running = false
    Cql::Future.completed
  end

  def running?
    @running
  end

  def add_connection(host, port)
    @default_host ||= host
    future = Cql::Future.new
    connection = {:host => host, :port => port, :future => future, :requests => []}
    @connections << connection
    future.complete!(connection.object_id) if @running
    future
  end

  def queue_request(request, connection_id=nil)
    if connection_id
      connection = @connections.find { |c| c.object_id == connection_id }
    else
      connection = @connections.sample
    end
    connection[:requests] << request
    response = @queued_responses[connection[:host]].shift || @queued_responses[nil].shift
    @last_used_connection = connection
    Cql::Future.completed([response, connection.object_id])
  end
end