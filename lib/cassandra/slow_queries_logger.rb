module Cassandra

  class SlowQueriesLogger

    def initialize(logger, threshold_ms = 250)
      @threshold_ms = threshold_ms
      @logger = logger
      @times = {}
      @requests = {}
      @logger.debug "Initialized slow queries logger with threshold: #{threshold_ms} ms."
    end

    def start(id, request)
      @times[id] = Time.now
      @requests[id] = request.respond_to?(:cql) ? request.cql : request.to_s
    end

    def finish(id)
      delta = ((Time.now - @times[id]) * 1_000).round 3
      if delta > @threshold_ms
        @logger.warn "***** SLOW QUERY!! *****"
        @logger.warn "#{@requests[id]} took #{delta} ms."
      end
    end

    def delete(id)
      @times.delete id
      @requests.delete id
    end
  end

  class NullQueriesLogger
    def initialize ; end

    def start(id, request) ; end

    def finish(id) ; end

    def delete(id) ; end
  end

  def self.queries_logger_for(logger)
    if logger.is_a? NullLogger
      NullQueriesLogger.new
    else
      SlowQueriesLogger.new logger
    end
  end
end
