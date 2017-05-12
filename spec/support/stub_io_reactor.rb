# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
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
#++

class StubIoReactor
  include MonitorMixin

  class NullObject
    def method_missing(method, *args, &block)
      self
    end

    def nil?
      true
    end
  end

  class Connection
    attr_reader :host
    attr_reader :port
    attr_reader :timeout
    attr_reader :ssl

    def initialize(host, port, timeout, ssl)
      @host      = host
      @port      = port
      @timeout   = timeout
      @ssl       = ssl
      @blocked   = false
      @connected = true
      @io        = NullObject.new

      @closed_listeners = []
      @incoming = ::Queue.new

      Thread.new do
        Thread.current.abort_on_exception = true
        loop do
          data = @incoming.pop
          break unless @connected
          sleep(rand)
          break unless @connected
          @data_listener.call(data)
        end
      end
    end

    def to_io
      @io
    end

    def connected?
      @connected
    end

    def closed?
      !@connected
    end

    def block
      @blocked = true
      self
    end

    def unblock
      @blocked = false
      self
    end

    def blocked?
      @blocked
    end

    def on_data(&block)
      @data_listener = block
      self
    end

    def on_closed(&block)
      @closed_listeners << block
      self
    end

    def write(bytes = nil, &block)
      return self if @blocked

      yield(bytes = Cassandra::Protocol::CqlByteBuffer.new) if block_given?
      decode(bytes)

      self
    end

    def close(cause = nil)
      @connected = false
      @incoming << nil
      @closed_listeners.each do |l|
        begin
          l.call(cause)
        rescue => e
          puts "#{e.class.name}: #{e.message}\n" + Array(e.backtrace).join("\n")
        end
      end.clear
      self
    end

    def receive(data)
      @incoming << data

      self
    end

    def inspect
      "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @blocked=#{@blocked}>"
    end

    private

    def decode(buffer)
      version = buffer.read_byte
      flags   = buffer.read_byte
      stream  = buffer.read_byte
      opcode  = buffer.read_byte
      length  = buffer.read_int

      compression = (flags & 0x01 == 0x01)
      tracing     = (flags & 0x02 == 0x02)

      request = case opcode
      when 0x01    # STARTUP
        options = buffer.read_string_map
        Cassandra::Protocol::StartupRequest.new(options['CQL_VERSION'], options['COMPRESSION'])
      when 0x0F    # AUTH_RESPONSE
        Cassandra::Protocol::AuthResponseRequest.new(buffer.read_bytes)
      when 0x05    # OPTIONS
        Cassandra::Protocol::OptionsRequest.new
      when 0x07    # QUERY
        cql                = buffer.read_long_string
        consistency        = buffer.read_consistency
        values             = nil
        serial_consistency = nil
        page_size          = nil
        paging_state       = nil

        if version > 1
          flags = buffer.read_byte

          if (flags & 0x01 == 0x01)
            values = Array.new(buffer.read_short) do
              size = buffer.read_int
              (size > 0) ? buffer.read(size) : nil
            end
          end

          page_size          = buffer.read_int         if (flags & 0x04 == 0x04)
          paging_state       = buffer.read_bytes       if (flags & 0x08 == 0x08)
          serial_consistency = buffer.read_consistency if (flags & 0x10 == 0x10)
        end

        Cassandra::Protocol::QueryRequest.new(cql, values, nil, consistency, serial_consistency, page_size, paging_state, tracing)
      when 0x09    # PREPARE
        Cassandra::Protocol::PrepareRequest.new(buffer.read_long_string, tracing)
      when 0x0A    # EXECUTE
        id                 = buffer.read_short_bytes
        metadata           = nil
        values             = nil
        request_metadata   = nil
        consistency        = nil
        serial_consistency = nil
        page_size          = nil
        paging_state       = nil

        if version > 1
          consistency = buffer.read_consistency
          flags       = buffer.read_byte

          if (flags & 0x01 == 0x01)
            values = Array.new(buffer.read_short) do
              size = buffer.read_int
              (size > 0) ? buffer.read(size) : nil
            end
          end

          page_size          = buffer.read_int         if (flags & 0x04 == 0x04)
          paging_state       = buffer.read_bytes       if (flags & 0x08 == 0x08)
          serial_consistency = buffer.read_consistency if (flags & 0x10 == 0x10)
        else
          values = Array.new(buffer.read_short) do
            size = buffer.read_int
            (size > 0) ? buffer.read(size) : nil
          end
        end

        Cassandra::Protocol::ExecuteRequest.new(id, metadata, values, request_metadata, consistency, serial_consistency, page_size, paging_state, tracing)
      when 0x0D    # BATCH
        type  = buffer.read_byte
        parts = Array.new(buffer.read_short) do
          case buffer.read_byte
          when 0 # QUERY
            cql = buffer.read_long_string

            buffer.read_short.times do
              size = buffer.read_int
              (size > 0) ? buffer.read(size) : nil
            end

            [:query, cql, nil, nil]
          when 1 # PREPARED
            id = buffer.read_short_bytes

            buffer.read_short.times do
              size = buffer.read_int
              (size > 0) ? buffer.read(size) : nil
            end

            [:prepared, id, [], []]
          end
        end
        consistency = buffer.consistency

        req = Cassandra::Protocol::BatchRequest.new(type, consistency, tracing)
        parts.each do |(method, *args)|
          req.__send__(:"add_#{method}", *args)
        end
        req
      when 0x0B    # REGISTER
        Cassandra::Protocol::RegisterRequest.new(*buffer.read_string_list)
      end

      handle(version, stream, request)
    end

    def encode(version, stream, response)
      buffer = Cassandra::Protocol::CqlByteBuffer.new

      case response
      when Cassandra::Protocol::ErrorResponse
        opcode = 0x00

        buffer.append_int(response.code)
        buffer.append_string(response.message)

        case response.code
        when 0x1000 # unavailable
          buffer.append_consistency(response.details[:cl])
          buffer.append_int(response.details[:required])
          buffer.append_int(response.details[:alive])
        when 0x1100 # write_timeout
          buffer.append_consistency(response.details[:cl])
          buffer.append_int(response.details[:received])
          buffer.append_int(response.details[:blockfor])
          buffer.append_string(response.details[:write_type].to_s.upcase)
        when 0x1200 # read_timeout
          buffer.append_consistency(response.details[:cl])
          buffer.append_int(response.details[:received])
          buffer.append_int(response.details[:blockfor])
          buffer.append(response.details[:data_present] ? Cassandra::Protocol::Constants::TRUE_BYTE : Cassandra::Protocol::Constants::FALSE_BYTE)
        when 0x2400 # already_exists
          buffer.append_string(response.details[:ks])
          buffer.append_string(response.details[:table])
        when 0x2500
          buffer.append_short_bytes(response.details[:id])
        end
      when Cassandra::Protocol::ReadyResponse
        opcode = 0x02
      when Cassandra::Protocol::AuthenticateResponse
        opcode = 0x03
        buffer.append_string(response.authentication_class)
      when Cassandra::Protocol::SupportedResponse
        opcode  = 0x06
        options = response.options

        buffer.append_short(options.size)
        options.each do |key, values|
          buffer.append_string(key)
          buffer.append_string_list(values)
        end

        self
      when Cassandra::Protocol::ResultResponse
        opcode = 0x08
        case response
        when Cassandra::Protocol::VoidResultResponse
          buffer.append_int(0x01)
        when Cassandra::Protocol::RowsResultResponse
          # buffer.append_int(0x02)
          buffer.append_int(0x01)
        when Cassandra::Protocol::SetKeyspaceResultResponse
          buffer.append_int(0x03)
          buffer.append_string(response.keyspace)
        when Cassandra::Protocol::PreparedResultResponse
          buffer.append_int(0x04)
          buffer.append_short_bytes(response.id)
          buffer.append_int(0)
          buffer.append_int(0)
        when Cassandra::Protocol::SchemaChangeResultResponse
          buffer.append_int(0x05)
          buffer.append_string(response.change)
          buffer.append_string(response.keyspace)
          buffer.append_string(response.name)
        end
      when EventResponse
        opcode = 0x0c

        case response
        when Cassandra::Protocol::SchemaChangeEventResponse
          buffer.append_string('SCHEMA_CHANGE')
          buffer.append_string(response.change)
          buffer.append_string(response.keyspace)
          buffer.append_string(response.name)
        when Cassandra::Protocol::StatusChangeEventResponse
          buffer.append_string('STATUS_CHANGE')
          buffer.append_string(response.change)
          buffer.append_int(response.address.ipv6? ? 16 : 4)
          buffer.append(response.address.hton)
          buffer.append_int(response.port)
        end
      when Cassandra::Protocol::AuthChallengeResponse
        opcode = 0x0e
        buffer.append_bytes(response.token)
      when Cassandra::Protocol::AuthSuccessResponse
        opcode = 0x10
        buffer.append_bytes(response.token)
      end

      [0x80 | version, 0, stream, opcode, buffer.bytesize].pack(Cassandra::Protocol::V1::Encoder::HEADER_FORMAT) + buffer
    end

    def handle(version, stream, request)
      case request
      when Cassandra::Protocol::StartupRequest
        receive(encode(version, stream, Cassandra::Protocol::ReadyResponse.new))
      when Cassandra::Protocol::QueryRequest
        receive(encode(version, stream, Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)))
      when Cassandra::Protocol::OptionsRequest
        receive(encode(version, stream, Cassandra::Protocol::SupportedResponse.new({'CQL_VERSION' => ['3.0.0'], 'COMPRESSION' => []})))
      else
        p request
      end
    end
  end

  class Timer
    def initialize(promise, expiration)
      @promise    = promise
      @expiration = expiration
      @expired    = false
    end

    def advance(seconds)
      if (@expiration - Time.now) <= seconds
        @promise.fulfill(object_id)
        @expired = true
      else
        @expiration -= seconds
      end
    end

    def resolves?(future)
      @promise.future == future
    end

    def expired?
      @expired
    end
  end

  attr_reader :connections
  attr_accessor :connection_options

  def initialize
    @enabled_nodes = ::Set.new
    @blocked_nodes = ::Set.new
    @connections   = ::Array.new
    @timers        = ::Array.new
    @max_conns     = ::Hash.new

    mon_initialize
  end

  def enable_nodes(ips)
    ips.each {|ip| enable_node(ip)}
    self
  end

  def enable_node(ip)
    @enabled_nodes << ip

    self
  end

  def disable_node(ip)
    @enabled_nodes.delete(ip)

    self
  end

  def block_nodes(ips)
    ips.each {|ip| block_node(ip)}
    self
  end

  def block_node(ip)
    @blocked_nodes << ip

    @connections.each do |connection|
      next unless connection.host == ip

      connection.block
    end

    self
  end

  def unblock_node(ip)
    @blocked_nodes.delete(ip)

    @connections.each do |connection|
      next unless connection.host == ip

      connection.unblock
    end

    self
  end

  def set_max_connections(ip, max)
    @max_conns[ip] = max
    self
  end

  def unset_max_connections(ip)
    @max_conns.delete(ip)
    self
  end

  def connect(host, port, options)
    if !@enabled_nodes.include?(host)
      Ione::Future.failed(Ione::Io::ConnectionError.new('Node down'))
    elsif @blocked_nodes.include?(host)
      Ione::Future.failed(Ione::Io::ConnectionTimeoutError.new('Node timed out'))
    else
      if @max_conns[host] && @connections.count {|c| c.host == host} >= @max_conns[host]
        Ione::Future.failed(Ione::Io::ConnectionTimeoutError.new('Max connections reached'))
      else
        @connections << connection = Connection.new(host, port, options[:timeout], options[:ssl])
        connection.on_closed do |cause|
          @connections.delete(connection)
        end
        handler = connection
        handler = yield(connection) if block_given?

        Ione::Future.resolved(handler)
      end
    end
  end

  def schedule_timer(seconds)
    synchronize do
      promise = Ione::Promise.new
      @timers << Timer.new(promise, Time.now + seconds)
      promise.future
    end
  end

  def advance_time(seconds)
    synchronize do
      @timers.dup.each { |timer| timer.advance(seconds) }
      @timers.reject! { |timer| timer.expired? }
    end
    self
  end

  def cancel_timer(timer_future)
    synchronize do
      @timers.reject! do |timer|
        timer.resolves?(timer_future)
      end
    end
  end
end
