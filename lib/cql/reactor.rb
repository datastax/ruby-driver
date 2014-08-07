# encoding: utf-8

module Cql
  # @private
  class Reactor
    def initialize(io_reactor)
      @io_reactor = io_reactor
    end

    def start
      @io_reactor.start
    end

    def stop
      @io_reactor.stop
    end

    def schedule_timer(timeout)
      @io_reactor.schedule_timer(timeout)
    end

    def connect(host, port, timeout, &block)
      @io_reactor.connect(host, port, timeout, &block)
    end
  end
end
