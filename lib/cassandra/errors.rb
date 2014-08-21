# encoding: utf-8

# Copyright 2013-2014 DataStax, Inc.
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

module Cassandra
  # Base class for all Errors raised by the driver 
  # @see Cassandra::Errors
  class Error < StandardError
  end

  module Errors
    # @!parse class IoError < StandardError; end
    # @private
    IoError = Ione::IoError

    # This error type represents errors sent by the server, the `code` attribute
    # can be used to find the exact type, and `cql` contains the request's CQL,
    # if any. `message` contains the human readable error message sent by the
    # server.
    class QueryError < Error
      # @return [Integer] error code
      attr_reader :code
      # @return [String] original CQL used
      attr_reader :cql
      # @return [Hash{Symbol => String, Integer}] various error details
      attr_reader :details

      # @private
      def initialize(code, message, cql=nil, details=nil)
        super(message)
        @code = code
        @cql = cql
        @details = details
      end
    end

    # This error is thrown when not hosts could be reached during connection or query execution.
    class NoHostsAvailable < Error
      # @return [Hash{Cassandra::Host => Exception}] a map of hosts to underlying exceptions
      attr_reader :errors

      # @private
      def initialize(errors = {})
        super("no hosts available, check #errors property for details")

        @errors = errors
      end
    end

    # Client error represents bad driver state or configuration
    #
    # @see Cassandra::Errors::AuthenticationError
    class ClientError < Error
    end

    # Raised when cannot authenticate to Cassandra
    class AuthenticationError < ClientError
    end

    # @private
    NotConnectedError = Class.new(Error)
    # @private
    NotPreparedError = Class.new(Error)
  end
end
