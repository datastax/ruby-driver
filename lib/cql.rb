# encoding: utf-8

require 'ione'

require 'monitor'
require 'ipaddr'
require 'set'
require 'bigdecimal'

module Cql
  CqlError = Class.new(StandardError)
  IoError = Ione::IoError

  # @private
  Promise = Ione::Promise

  # @private
  Future = Ione::Future

  class Future
    def get
      value
    end
  end

  # @private
  Io = Ione::Io

  def self.cluster(defaults = {})
    Builder.new(defaults)
  end
end

require 'cql/uuid'
require 'cql/time_uuid'
require 'cql/compression'
require 'cql/protocol'
require 'cql/auth'
require 'cql/client'

require 'cql/builder'
require 'cql/cluster'
require 'cql/driver'
require 'cql/host'
require 'cql/reactor'
require 'cql/session'
require 'cql/statement'
require 'cql/statements'

require 'cql/load_balancing'
require 'cql/reconnection'
require 'cql/retry'

module Cql
  # @private
  VOID_STATEMENT = Statements::Void.new
  # @private
  NO_HOSTS       = NoHostsAvailable.new
end
