class SchemaChangeListener
  class Condition
    attr_reader :promise

    def initialize(promise, &block)
      @promise = promise
      @block   = block
    end

    def evaluate(keyspace)
      result = @block.call(keyspace)
      @promise.fulfill(result) if result
    rescue => e
      @promise.break(e)
    end
  end

  def initialize
    @conditions = {}
  end

  def wait_for_change(keyspace, timeout = nil, &block)
    # First run the block and see if it succeeds; if so, there's nothing
    # to wait for.
    result = block.call(keyspace)
    return result if result

    # Ok, looks like we do need to wait...
    promise = Cassandra::Future.promise
    @conditions[keyspace.name] ||= []
    @conditions[keyspace.name] << Condition.new(promise, &block)

    promise.future.get(timeout)
  ensure
    @conditions[keyspace.name].reject! { |c| c.promise == promise }
  end

  def keyspace_changed(keyspace)
    @conditions.fetch(keyspace.name) { return }.each { |c| c.evaluate(keyspace) }
    nil
  end
end