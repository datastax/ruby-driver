# encoding: utf-8

#--
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
#++

module Cassandra
  module Protocol
    class StartupRequest < Request
      attr_reader :options

      def initialize(cql_version, compression=nil)
        super(1)
        raise ArgumentError, "Invalid CQL version: #{cql_version.inspect}" unless cql_version
        @options = {CQL_VERSION => cql_version}
        @options[COMPRESSION] = compression if compression
      end

      def compressable?
        false
      end

      def write(buffer, protocol_version, encoder)
        buffer.append_string_map(@options)
      end

      def to_s
        %(STARTUP #@options)
      end

      private

      CQL_VERSION = 'CQL_VERSION'.freeze
      COMPRESSION = 'COMPRESSION'.freeze
    end
  end
end
