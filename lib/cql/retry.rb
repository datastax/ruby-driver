# encoding: utf-8

require 'cql/retry/decisions'
require 'cql/retry/policy'
require 'cql/retry/policies'

module Cql
  module Retry
    DECISION_RERAISE = Decisions::Reraise.new
    DECISION_IGNORE  = Decisions::Ignore.new
  end
end
