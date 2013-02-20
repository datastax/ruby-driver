# encoding: utf-8

module AwaitHelper
  def await(timeout=5, &test)
    started_at = Time.now
    until test.call
      yield
      time_taken = Time.now - started_at
      if time_taken > timeout
        fail('Test took more than %.1fs' % [time_taken.to_f])
      else
        sleep(0.01)
      end
    end
  end
end

RSpec.configure do |c|
  c.include(AwaitHelper)
end