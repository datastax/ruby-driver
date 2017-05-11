# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
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
  # Represents a cassandra user defined function
  # @see Cassandra::Keyspace#each_function
  # @see Cassandra::Keyspace#function
  # @see Cassandra::Keyspace#has_function?
  class Function
    # @private
    attr_reader :keyspace
    # @return [String] function name
    attr_reader :name
    # @return [String] function language
    attr_reader :language
    # @return [Cassandra::Type] function return type
    attr_reader :type
    # @return [String] function body
    attr_reader :body

    # @private
    def initialize(keyspace, name, language, type, arguments, body, called_on_null)
      @keyspace       = keyspace
      @name           = name
      @language       = language
      @type           = type
      @arguments      = arguments
      @body           = body
      @called_on_null = called_on_null

      # Build up an arguments hash keyed on arg-name.
      @arguments_hash = @arguments.each_with_object({}) do |arg, h|
        h[arg.name] = arg
      end
    end

    # @return [Boolean] whether this function will be called on null input
    def called_on_null?
      @called_on_null
    end

    # @param name [String] argument name
    # @return [Boolean] whether this function has a given argument
    def has_argument?(name)
      @arguments_hash.key?(name)
    end

    # @param name [String] argument name
    # @return [Cassandra::Argument, nil] an argument or nil
    def argument(name)
      @arguments_hash[name]
    end

    # Yield or enumerate each argument defined in this function
    # @overload each_argument
    #   @yieldparam argument [Cassandra::Argument] current argument
    #   @return [Cassandra::Table] self
    # @overload each_argument
    #   @return [Array<Cassandra::Argument>] a list of arguments
    def each_argument(&block)
      if block_given?
        @arguments.each(&block)
        self
      else
        # We return a dup of the arguments so that the caller can manipulate
        # the array however they want without affecting the source.
        @arguments.dup
      end
    end
    alias arguments each_argument

    # Get the list of argument types for this function.
    # @return [Array<Cassandra::Type>] a list of argument types.
    def argument_types
      @arguments.map(&:type)
    end

    # @private
    def eql?(other)
      other.is_a?(Function) && \
        @keyspace == other.keyspace && \
        @name == other.name && \
        @language == other.language && \
        @type == other.type && \
        @arguments == other.arguments && \
        @body == other.body && \
        @called_on_null == other.called_on_null?
    end
    alias == eql?

    # @private
    def hash
      @hash ||= begin
        h = 17
        h = 31 * h + @keyspace.hash
        h = 31 * h + @name.hash
        h = 31 * h + @language.hash
        h = 31 * h + @type.hash
        h = 31 * h + @arguments.hash
        h = 31 * h + @body.hash
        h = 31 * h + @called_on_null.hash
        h
      end
    end

    # @private
    def inspect
      "#<Cassandra::Function:0x#{object_id.to_s(16)} " \
          "@keyspace=#{@keyspace.inspect}, " \
          "@name=#{@name.inspect}, " \
          "@language=#{@language.inspect}, " \
          "@type=#{@type.inspect}, " \
          "@arguments=#{@arguments.inspect} " \
          "@body=#{@body.inspect}>"
    end

    # @return [String] a cql representation of this function
    def to_cql
      cql = "CREATE FUNCTION #{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)}("
      first = true
      @arguments.each do |argument|
        if first
          first = false
        else
          cql << ', '
        end
        cql << "#{argument.name} #{argument.type}"
      end
      cql << ')'
      cql << if @called_on_null
               "\n  CALLED ON NULL INPUT"
             else
               "\n  RETURNS NULL ON NULL INPUT"
             end
      cql << "\n  RETURNS #{@type}"
      cql << "\n  LANGUAGE #{@language}"
      cql << "\n  AS $$#{@body}$$"
      cql << ';'
    end
  end
end
