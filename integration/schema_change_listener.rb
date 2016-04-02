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

  def initialize(cluster)
    @cluster = cluster
    @conditions = {}
    @cluster.register(self)
  end

  def wait_for_change(keyspace_name, timeout = nil, &block)
    # First run the block and see if it succeeds; if so, there's nothing
    # to wait for.
    result = block.call(@cluster.keyspace(keyspace_name))
    return result if result

    # Ok, looks like we do need to wait...
    begin
      promise = Cassandra::Future.promise
      @conditions[keyspace_name] ||= []
      @conditions[keyspace_name] << Condition.new(promise, &block)

      promise.future.get(timeout)
    ensure
      # Clean up the fulfilled/broken promise (e.g. the Condition associated
      # with the promise)
      @conditions[keyspace_name].reject! { |c| c.promise == promise }
    end
  end


  def wait_for_function(keyspace_name, function_name, *args)
    wait_for_change(keyspace_name, 2) do |ks|
      ks.has_function?(function_name, *args)
    end
  end

  def wait_for_aggregate(keyspace_name, aggregate_name, *args)
    wait_for_change(keyspace_name, 2) do |ks|
      ks.has_aggregate?(aggregate_name, *args)
    end
  end

  def wait_for_table(keyspace_name, table_name, *args)
    wait_for_change(keyspace_name, 2) do |ks|
      ks.has_table?(table_name, *args)
    end
  end

  def wait_for_index(keyspace_name, table_name, index_name, *args)
    wait_for_change(keyspace_name, 2) do |ks|
      ks.table(table_name).has_index?(index_name, *args)
    end
  end

  def keyspace_changed(keyspace)
    # This looks a little strange, but here's the idea: if we don't have
    # Condition's for this keyspace, immediately return. Otherwise, for each
    # Condition, evaluate with the keyspace.
    @conditions.fetch(keyspace.name) { return }.each { |c| c.evaluate(keyspace) }
    nil
  end
end