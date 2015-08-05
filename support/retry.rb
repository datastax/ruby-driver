module Retry extend self
  def with_attempts(attempts, errors = [Cassandra::Errors::ValidationError, Cassandra::Errors::ExecutionError])
    total ||= attempts + 1
    return yield
  rescue *errors => e
    raise e if (attempts -= 1).zero?
    wait = (total - attempts) * 0.4
    puts "#{e.class.name}: #{e.message}, retrying in #{wait}s..."
    sleep(wait)
    retry
  end
end
