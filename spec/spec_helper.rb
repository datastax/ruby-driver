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
    server_state[:lock] = Mutex.new
    server_state[:running] = true
    server_state[:connects] = 0
    server_state[:disconnects] = 0
    server_state[:response_bytes] = response_bytes
    server_state[:received_bytes] = ''
    server_state[:sockets] = [TCPServer.new(port)]
    server_state[:thread] = Thread.start do
      Thread.current.abort_on_exception = true
      lock = server_state[:lock]
      connections = []
      while server_state[:running]
        acceptables, _ = IO.select(server_state[:sockets], connections, nil, 0)
        readables, writables, exceptionals = IO.select(connections, connections, connections, 0)

        if exceptionals && exceptionals.any?
          p [:exceptionals, exceptionals]
        end

        if acceptables
          acceptables.each do |socket|
            connection, _ = socket.accept_nonblock
            lock.synchronize do
              server_state[:connects] += 1
            end
            connections << connection
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
              connections.delete(readable)
              lock.synchronize do
                server_state[:disconnects] += 1
              end
            end
          end
        end

        if writables && server_state[:response_bytes] && server_state[:response_bytes].size > 0
          writables.each do |writable|
            lock.synchronize do
              n = writable.write_nonblock(server_state[:response_bytes])
              server_state[:response_bytes].slice!(0, n)
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
end