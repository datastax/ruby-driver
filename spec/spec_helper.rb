# encoding: utf-8

require 'bundler/setup'
require 'simplecov'; SimpleCov.start
require 'cql'

ENV['CASSANDRA_HOST'] ||= 'localhost'


module AsyncHelpers
  def await(timeout=2)
    timeout_token = rand(2131234)
    lock = Queue.new
    begin
      yield lock
    ensure
      Thread.start do
        sleep(timeout)
        lock << timeout_token
      end
      lock.pop.should_not equal(timeout_token), 'test timed out'
    end
  end
end

module FakeServerHelpers
  def start_server!(port, response_bytes=nil)
    @server_lock = Mutex.new
    @server_running = [true]
    @connects = []
    @disconnects = []
    @data = ''
    @sockets = [TCPServer.new(port)]
    @server_thread = Thread.start(@sockets, @server_running, @connects, @disconnects, @data, @server_lock) do |sockets, server_running, connects, disconnects, data, lock|
      begin
        Thread.current.abort_on_exception = true
        while server_running[0]
          readables, _ = IO.select(sockets, nil, nil, 0)
          if readables
            readables.each do |socket|
              connection, _ = socket.accept_nonblock
              lock.synchronize do
                connects << 1
              end
              connection.write(response_bytes) if response_bytes
              bytes = connection.read
              lock.synchronize do
                data << bytes
                disconnects << 1
              end
              connection.close
            end
          end
        end
      end
    end
  end

  def stop_server!
    return unless @server_running[0]
    @server_running[0] = false
    @server_thread.join
    @sockets.each(&:close)
  end

  def server_stats
    stats = {}
    @server_lock.synchronize do
      stats[:data] = @data
      stats[:connects] = @connects ? @connects.size : 0
      stats[:disconnects] = @disconnects ? @disconnects.size : 0
    end
    stats
  end
end