# encoding: utf-8

require 'cassandra'
require 'ione'

class PreparedStatement
  attr_reader :statement

  def initialize(client, statement)
    @client = client
    @statement = statement
  end

  def execute(*args)
    @client.execute(@statement, *args)
  end
end

class BatchStatement
  def initialize(client, batch)
    @client = client
    @batch = batch
  end

  def execute(options = {})
    @client.execute(@batch, options)
  end

  def add(*args)
    @batch.add(*args)
    self
  end
end

class Client
  def initialize(session)
    @session = session
  end

  def execute(*args)
    future = Ione::CompletableFuture.new
    @session.execute_async(*args).on_complete do |e, v|
      if e
        future.fail(e)
      else
        future.resolve(v)
      end
    end
    future
  end

  def prepare(statement, options = {})
    future = Ione::CompletableFuture.new
    @session.prepare_async(statement, options).on_complete do |e, v|
      if e
        future.fail(e)
      else
        future.resolve(PreparedStatement.new(self, v))
      end
    end
    future
  end

  def batch(type = :logged, options = {})
    batch = BatchStatement.new(self, @session.send(:"#{type}_batch"))
    if block_given?
      yield(batch)
      batch.execute(options)
    else
      batch
    end
  end

  def close
    future = Ione::CompletableFuture.new
    @session.close.on_complete do |e, v|
      if e
        future.fail(e)
      else
        future.resolve(v)
      end
    end
    future
  end
end

cluster = Cassandra.connect
session = cluster.connect
client  = Client.new(session)
