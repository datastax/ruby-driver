# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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
#++

module Cassandra
  # Included in all Errors raised by the driver to allow rescuing from any
  # driver-specific error.
  # @example Catching all driver errors
  #   begin
  #     cluster = Cassandra.cluster
  #     session = cluster.connect
  #   rescue Cassandra::Error => e
  #     puts "#{e.class.name}: #{e.message}"
  #   end
  # @see Cassandra::Errors
  module Error
  end

  module Errors
    # Mixed into any host-specific errors. Requests resulting in these errors
    # are attempted on other hosts. Once all hosts failed with this type of
    # error, a {Cassandra::Errors::NoHostsAvailable} error is raised with
    # details of failures.
    #
    # @see Errors::NoHostsAvailable
    module HostError
    end

    # Raised when something unexpected happened. This indicates a server-side
    # bug.
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L654-L655 Description
    #   of Server Error in Apache Cassandra native protocol spec v1
    class ServerError < ::StandardError
      include Error, HostError

      # @private
      def initialize(message,
                     payload,
                     warnings,
                     keyspace,
                     statement,
                     options,
                     hosts,
                     consistency,
                     retries)
        super(message)
        @payload     = payload
        @warnings    = warnings
        @keyspace    = keyspace
        @statement   = statement
        @options     = options
        @hosts       = hosts
        @consistency = consistency
        @retries     = retries
      end

      # Query execution information, such as number of retries and all tried hosts, etc.
      # @return [Cassandra::Execution::Info]
      def execution_info
        @info ||= Execution::Info.new(@payload,
                                      @warnings,
                                      @keyspace,
                                      @statement,
                                      @options,
                                      @hosts,
                                      @consistency,
                                      @retries,
                                      nil)
      end
    end

    # Mixed into all internal driver errors.
    class InternalError < ::RuntimeError
      include Error, HostError
    end

    # Raised when data decoding fails.
    class DecodingError < ::RuntimeError
      include Error, HostError
    end

    # Raised when data encoding fails.
    class EncodingError < ::RuntimeError
      include Error, HostError
    end

    # Raised when a connection level error occured.
    class IOError < ::IOError
      include Error, HostError
    end

    # Raised when a timeout has occured.
    class TimeoutError < ::Timeout::Error
      include Error
    end

    # Mixed into all request execution errors.
    module ExecutionError
      include Error

      # @private
      def initialize(message,
                     payload,
                     warnings,
                     keyspace,
                     statement,
                     options,
                     hosts,
                     consistency,
                     retries)
        super(message)
        @payload     = payload
        @warnings    = warnings
        @keyspace    = keyspace
        @statement   = statement
        @options     = options
        @hosts       = hosts
        @consistency = consistency
        @retries     = retries
      end

      # Query execution information, such as number of retries and all tried hosts, etc.
      # @return [Cassandra::Execution::Info]
      def execution_info
        @info ||= Execution::Info.new(@payload,
                                      @warnings,
                                      @keyspace,
                                      @statement,
                                      @options,
                                      @hosts,
                                      @consistency,
                                      @retries,
                                      nil)
      end
    end

    # Raised when coordinator determines that a request cannot be executed
    # because there are not enough replicas. In this scenario, the request is
    # not sent to the nodes at all.
    #
    # @note This error can be handled by a {Cassandra::Retry::Policy} to
    #   determine the desired outcome.
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L662-L672 Description
    #   of Unavailable Error in Apache Cassandra native protocol spec v1
    class UnavailableError < ::StandardError
      include ExecutionError
      # Consistency level that triggered the error.
      #
      # @return [Symbol] the original consistency level for the request, one of
      #   {Cassandra::CONSISTENCIES}
      attr_reader :consistency

      # @return [Integer] the number of replicas required to achieve requested
      #   consistency level
      attr_reader :required

      # @return [Integer] the number of replicas available for the request
      attr_reader :alive

      # @private
      def initialize(message,
                     payload,
                     warnings,
                     keyspace,
                     statement,
                     options,
                     hosts,
                     r_consistency,
                     retries,
                     consistency,
                     required,
                     alive)
        super(message,
              payload,
              warnings,
              keyspace,
              statement,
              options,
              hosts,
              r_consistency,
              retries)
        @consistency = consistency
        @required    = required
        @alive       = alive
      end
    end

    # Raised when the request cannot be processed because the coordinator node
    # is overloaded
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L673-L674 Description
    #   of Overloaded Error in Apache Cassandra native protocol spec v1
    class OverloadedError < ::StandardError
      include ExecutionError, HostError
    end

    # Raise when the request was a read request but the coordinator node is
    # bootstrapping
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L675-L676 Description
    #   of Is Bootstrapping Error in Apache Cassandra native protocol spec v1
    class IsBootstrappingError < ::StandardError
      include ExecutionError, HostError
    end

    # Raised when truncation failed.
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L677 Description of
    #   Truncate Error in Apache Cassandra native protocol spec v1
    class TruncateError < ::StandardError
      include ExecutionError
    end

    # Raised when a write request timed out.
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L678-L703 Description
    #   of Write Timeout Error in Apache Cassandra native protocol spec v1
    class WriteTimeoutError < ::StandardError
      include ExecutionError

      # @return [Symbol] the type of write request that timed out, one of
      #   {Cassandra::WRITE_TYPES}
      attr_reader :type
      # @return [Symbol] the original consistency level for the request, one of
      #   {Cassandra::CONSISTENCIES}
      attr_reader :consistency
      # @return [Integer] the number of acks required to achieve requested
      #   consistency level
      attr_reader :required
      # @return [Integer] the number of acks received by the time the query
      #   timed out
      attr_reader :received

      # @private
      def initialize(message,
                     payload,
                     warnings,
                     keyspace,
                     statement,
                     options,
                     hosts,
                     r_consistency,
                     retries,
                     type,
                     consistency,
                     required,
                     received)
        super(message,
              payload,
              warnings,
              keyspace,
              statement,
              options,
              hosts,
              r_consistency,
              retries)
        @type        = type
        @consistency = consistency
        @required    = required
        @received    = received
      end
    end

    # Raised when a read request timed out.
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L704-L721 Description
    #   of Read Timeout Error in Apache Cassandra native protocol spec v1
    class ReadTimeoutError < ::StandardError
      include ExecutionError

      # @return [Boolean] whether actual data (as opposed to data checksum) was
      #   present in the received responses.
      attr_reader :retrieved
      alias retrieved? retrieved

      # @return [Symbol] the original consistency level for the request, one of
      #   {Cassandra::CONSISTENCIES}
      attr_reader :consistency
      # @return [Integer] the number of responses required to achieve requested
      #   consistency level
      attr_reader :required
      # @return [Integer] the number of responses received by the time the
      #   query timed out
      attr_reader :received

      # @private
      def initialize(message,
                     payload,
                     warnings,
                     keyspace,
                     statement,
                     options,
                     hosts,
                     r_consistency,
                     retries,
                     retrieved,
                     consistency,
                     required,
                     received)
        super(message,
              payload,
              warnings,
              keyspace,
              statement,
              options,
              hosts,
              r_consistency,
              retries)
        @retrieved   = retrieved
        @consistency = consistency
        @required    = required
        @received    = received
      end
    end

    # Raised when a write request fails.
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-3.4/doc/native_protocol_v4.spec#L1106-L1134 Description
    #   of Write Failure Error in Apache Cassandra native protocol spec v4
    class WriteError < ::StandardError
      include ExecutionError

      # @return [Symbol] the type of write request that timed out, one of
      #   {Cassandra::WRITE_TYPES}
      attr_reader :type
      # @return [Symbol] the original consistency level for the request, one of
      #   {Cassandra::CONSISTENCIES}
      attr_reader :consistency
      # @return [Integer] the number of acks required
      attr_reader :required
      # @return [Integer] the number of acks received
      attr_reader :received
      # @return [Integer] the number of writes failed
      attr_reader :failed
      # @return [Hash<IPAddr, Integer>] map of <ip, error-code>. This is new in v5 and is nil in previous versions
      #    of the Casssandra protocol.
      attr_reader :failures_by_node

      # @private
      def initialize(message,
                     payload,
                     warnings,
                     keyspace,
                     statement,
                     options,
                     hosts,
                     r_consistency,
                     retries,
                     type,
                     consistency,
                     required,
                     failed,
                     received,
                     failures_by_node)
        super(message,
              payload,
              warnings,
              keyspace,
              statement,
              options,
              hosts,
              r_consistency,
              retries)
        @type        = type
        @consistency = consistency
        @required    = required
        @failed      = failed
        @received    = received
        @failures_by_node = failures_by_node
      end
    end

    # Raised when a read request fails.
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-3.4/doc/native_protocol_v4.spec#L1084-L1098 Description
    #   of Read Failure Error in Apache Cassandra native protocol spec v4
    class ReadError < ::StandardError
      include ExecutionError

      # @return [Boolean] whether actual data (as opposed to data checksum) was
      #   present in the received responses.
      attr_reader :retrieved
      # @return [Symbol] the original consistency level for the request, one of
      #   {Cassandra::CONSISTENCIES}
      attr_reader :consistency
      # @return [Integer] the number of responses required
      attr_reader :required
      # @return [Integer] the number of responses received
      attr_reader :received
      # @return [Integer] the number of reads failed
      attr_reader :failed
      # @return [Hash<IPaddr, Integer>] map of <ip, error-code>. This is new in v5 and is nil in previous versions
      #    of the Casssandra protocol.
      attr_reader :failures_by_node

      # @private
      def initialize(message,
                     payload,
                     warnings,
                     keyspace,
                     statement,
                     options,
                     hosts,
                     r_consistency,
                     retries,
                     retrieved,
                     consistency,
                     required,
                     failed,
                     received,
                     failures_by_node)
        super(message,
              payload,
              warnings,
              keyspace,
              statement,
              options,
              hosts,
              r_consistency,
              retries)
        @retrieved   = retrieved
        @consistency = consistency
        @required    = required
        @failed      = failed
        @received    = received
        @failures_by_node = failures_by_node
      end

      def retrieved?
        @retrieved
      end
    end

    # Raised when function execution fails.
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-3.4/doc/native_protocol_v4.spec#L1099-L1105 Description
    #   of Function Failure Error in Apache Cassandra native protocol spec v4
    class FunctionCallError < ::StandardError
      include ExecutionError

      # @return [String] keyspace
      attr_reader :keyspace
      # @return [String] name
      attr_reader :name
      # @return [String] signature
      attr_reader :signature

      # @private
      def initialize(message,
                     payload,
                     warnings,
                     r_keyspace,
                     statement,
                     options,
                     hosts,
                     consistency,
                     retries,
                     keyspace,
                     name,
                     signature)
        super(message,
              payload,
              warnings,
              r_keyspace,
              statement,
              options,
              hosts,
              consistency,
              retries)
        @keyspace  = keyspace
        @name      = name
        @signature = signature
      end
    end

    # Client error represents bad driver state or mis-configuration
    class ClientError < ::StandardError
      include Error
    end

    # Raised when some client message triggered a protocol violation (for
    # instance a QUERY message is sent before a STARTUP one has been sent)
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L656-L658 Description
    #   of Protocol Error in Apache Cassandra native protocol spec v1
    class ProtocolError < ClientError
      # @private
      def initialize(message,
                     payload,
                     warnings,
                     keyspace,
                     statement,
                     options,
                     hosts,
                     consistency,
                     retries)
        super(message)
        @payload     = payload
        @warnings    = warnings
        @keyspace    = keyspace
        @statement   = statement
        @options     = options
        @hosts       = hosts
        @consistency = consistency
        @retries     = retries
      end

      # Query execution information, such as number of retries and all tried hosts, etc.
      # @return [Cassandra::Execution::Info]
      def execution_info
        @info ||= Execution::Info.new(@payload,
                                      @warnings,
                                      @keyspace,
                                      @statement,
                                      @options,
                                      @hosts,
                                      @consistency,
                                      @retries,
                                      nil)
      end
    end

    # Raised when cannot authenticate to Cassandra
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L659-L660 Description
    #   of Bad Credentials Error in Apache Cassandra native protocol spec v1
    class AuthenticationError < ClientError
      # @private
      def initialize(message,
                     payload,
                     warnings,
                     keyspace,
                     statement,
                     options,
                     hosts,
                     consistency,
                     retries)
        super(message)
        @payload     = payload
        @warnings    = warnings
        @keyspace    = keyspace
        @statement   = statement
        @options     = options
        @hosts       = hosts
        @consistency = consistency
        @retries     = retries
      end

      # Query execution information, such as number of retries and all tried hosts, etc.
      # @return [Cassandra::Execution::Info]
      def execution_info
        @info ||= Execution::Info.new(@payload,
                                      @warnings,
                                      @keyspace,
                                      @statement,
                                      @options,
                                      @hosts,
                                      @consistency,
                                      @retries,
                                      nil)
      end
    end

    # Mixed into all request validation errors.
    module ValidationError
      include Error

      # @private
      def initialize(message,
                     payload,
                     warnings,
                     keyspace,
                     statement,
                     options,
                     hosts,
                     consistency,
                     retries)
        super(message)
        @payload     = payload
        @warnings    = warnings
        @keyspace    = keyspace
        @statement   = statement
        @options     = options
        @hosts       = hosts
        @consistency = consistency
        @retries     = retries
      end

      # Query execution information, such as number of retries and all tried hosts, etc.
      # @return [Cassandra::Execution::Info]
      def execution_info
        @info ||= Execution::Info.new(@payload,
                                      @warnings,
                                      @keyspace,
                                      @statement,
                                      @options,
                                      @hosts,
                                      @consistency,
                                      @retries,
                                      nil)
      end
    end

    # Raised when a prepared statement tries to be executed and the provided
    # prepared statement ID is not known by this host
    #
    # @note Seeing this error can be considered a Ruby Driver bug as it should
    #   handle automatic re-preparing internally.
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L738-L741 Description
    #   of Unprepared Error in Apache Cassandra native protocol spec v1
    class UnpreparedError < ::StandardError
      include ValidationError
      # @return [String] prepared statement id that triggered the error
      attr_reader :id

      # @private
      def initialize(message,
                     payload,
                     warnings,
                     keyspace,
                     statement,
                     options,
                     hosts,
                     consistency,
                     retries,
                     id)
        super(message,
              payload,
              warnings,
              keyspace,
              statement,
              options,
              hosts,
              consistency,
              retries)
        @id = id
      end
    end

    # Raised when the submitted query has a syntax error.
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L723 Description of
    #   Syntax Error in Apache Cassandra native protocol spec v1
    class SyntaxError < ::StandardError
      include ValidationError
    end

    # Raised when the logged user doesn't have the right to perform the query.
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L724-L725 Description
    #   of Unauthorized Error in Apache Cassandra native protocol spec v1
    class UnauthorizedError < ::StandardError
      include ValidationError
    end

    # Raised when the query is syntactically correct but invalid.
    #
    # @example Creating a table without selecting a keyspace
    #   begin
    #     session.execute("CREATE TABLE users (user_id INT PRIMARY KEY)")
    #   rescue Cassandra::Errors::InvalidError
    #   end
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L726 Description
    #   of Invalid Error in Apache Cassandra native protocol spec v1
    class InvalidError < ::StandardError
      include ValidationError
    end

    # Raised when the query is invalid because of some configuration issue.
    #
    # @example Dropping non-existent keyspace
    #   begin
    #     client.execute("DROP KEYSPACE unknown_keyspace")
    #   rescue Cassandra::Errors::ConfigurationError
    #   end
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L727 Description of
    #   Config Error in Apache Cassandra native protocol spec v1
    class ConfigurationError < ::StandardError
      include ValidationError
    end

    # Raised when the query attempted to create a keyspace or a table that was
    # already existing.
    #
    # @example Creating a table twice
    #   session.execute("USE my_keyspace")
    #   session.execute("CREATE TABLE users (user_id INT PRIMARY KEY)")
    #   begin
    #     session.execute("CREATE TABLE users (user_id INT PRIMARY KEY)")
    #   rescue Cassandra::Errors::AlreadyExistsError => e
    #     p ['already exists', e.keyspace, e.table]
    #   end
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L728-L737 Description
    #   of Already Exists Error in Apache Cassandra native protocol spec v1
    class AlreadyExistsError < ConfigurationError
      # @return [String] keyspace
      attr_reader :keyspace

      # @return [String, nil] table or `nil`
      attr_reader :table

      # @private
      def initialize(message,
                     payload,
                     warnings,
                     r_keyspace,
                     statement,
                     options,
                     hosts,
                     consistency,
                     retries,
                     keyspace,
                     table)
        super(message,
              payload,
              warnings,
              r_keyspace,
              statement,
              options,
              hosts,
              consistency,
              retries)
        @keyspace = keyspace
        @table    = table
      end
    end

    # This error is thrown when all attempted hosts raised a
    # {Cassandra::Errors::HostError} during connection or query execution.
    #
    # @see Cassandra::Cluster#connect
    # @see Cassandra::Session#execute
    class NoHostsAvailable < ::StandardError
      include Error

      # @return [Hash{Cassandra::Host => Cassandra::Errors::HostError}] a map
      #   of hosts to underlying exceptions
      attr_reader :errors

      # @private
      def initialize(errors = nil)
        if errors
          first   = true
          message = 'All attempted hosts failed'
          errors.each do |(host, error)|
            if first
              first = false
              message << ': '
            else
              message << ', '
            end
            message << "#{host.ip} (#{error.class.name}: #{error.message})"
          end
        else
          message = 'All hosts down'
        end

        super(message)

        @errors = errors || {}
      end
    end
  end
end
