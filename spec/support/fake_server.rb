# encoding: utf-8

class FakeServer
  attr_reader :port, :connects, :disconnects

  def initialize(port=(2**15 + rand(2**15)))
    @port = port
    @state = {}
    @lock = Mutex.new
    @connects = 0
    @disconnects = 0
    @connections = []
    @received_bytes = ''
  end

  def start!(options={})
    @lock.synchronize do
      return if @running
      @running = true
    end
    @sockets = [TCPServer.new(@port)]
    @started = Cql::Promise.new
    @thread = Thread.start do
      Thread.current.abort_on_exception = true
      sleep(options[:accept_delay] || 0)
      @started.fulfill
      io_loop
    end
    @started.future.value
    self
  end

  def stop!
    @lock.synchronize do
      return unless @running
      @running = false
    end
    if defined? @started
      @thread.join
      @sockets.each(&:close)
    end
  end

  def broadcast!(bytes)
    @lock.synchronize do
      @connections.each { |c| c.write_nonblock(bytes) }
    end
  end

  def await_connects!(n=1)
    started_at = Time.now
    until @connects >= n
      sleep(0.01)
      raise 'Waited longer than 5s!' if (Time.now - started_at) > 5
    end
  end

  def await_disconnects!(n=1)
    started_at = Time.now
    until @disconnects >= n
      sleep(0.01)
      raise 'Waited longer than 5s!' if (Time.now - started_at) > 5
    end
  end

  def received_bytes
    @lock.synchronize do
      return @received_bytes.dup
    end
  end

  private

  def io_loop
    while @running
      acceptables, _ = IO.select(@sockets, @connections, nil, 0)
      readables, writables, _ = IO.select(@connections, @connections, nil, 0)

      if acceptables
        acceptables.each do |socket|
          connection, _ = socket.accept_nonblock
          @lock.synchronize do
            @connects += 1
            @connections << connection
          end
        end
      end

      if readables
        readables.each do |readable|
          begin
            bytes = readable.read_nonblock(2**16)
            @lock.synchronize do
              @received_bytes << bytes
            end
          rescue EOFError
            @lock.synchronize do
              @connections.delete(readable)
              @disconnects += 1
            end
          end
        end
      end
    end
  end
end