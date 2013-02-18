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
    @fake_servers ||= {}
    raise "Port in use: #{port}" if @fake_servers[port]
    @fake_servers[port] = server_state = {}
    server_state[:port] = port
    server_state[:lock] = Mutex.new
    server_state[:running] = true
    server_state[:connects] = 0
    server_state[:disconnects] = 0
    server_state[:connections] = []
    server_state[:response_bytes] = response_bytes
    server_state[:received_bytes] = ''
    server_state[:sockets] = [TCPServer.new(port)]
    server_state[:thread] = Thread.start do
      Thread.current.abort_on_exception = true
      lock = server_state[:lock]
      while server_state[:running]
        connections = nil

        lock.synchronize do
          connections = server_state[:connections].dup
        end

        acceptables, _ = IO.select(server_state[:sockets], connections, nil, 0)
        readables, writables, _ = IO.select(connections, connections, nil, 0)

        if acceptables
          acceptables.each do |socket|
            connection, _ = socket.accept_nonblock
            lock.synchronize do
              server_state[:connects] += 1
              server_state[:connections] << connection
            end
          end
        end

        if readables
          readables.each do |readable|
            begin
              bytes = readable.read_nonblock(2**16)
              lock.synchronize do
                server_state[:received_bytes] << bytes
              end
            rescue EOFError
              lock.synchronize do
                server_state[:connections].delete(readable)
                server_state[:disconnects] += 1
              end
            end
          end
        end
      end
    end
  end

  def stop_server!(port)
    server_state = @fake_servers.delete(port)
    return unless server_state
    server_state[:running] = false
    server_state[:thread].join
    server_state[:sockets].each(&:close)
  end

  def server_stats(port)
    @fake_servers[port]
  end

  def server_broadcast!(port, bytes)
    server_stats(port)[:lock].synchronize do
      server_stats(port)[:connections].each { |c| c.write_nonblock(bytes) }
    end
  end
end

require 'support/fake_io_reactor'