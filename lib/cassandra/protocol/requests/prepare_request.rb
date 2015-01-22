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
    class PrepareRequest < Request
      attr_reader :cql
      attr_accessor :consistency, :retries

      def initialize(cql, trace=false)
        raise ArgumentError, 'No CQL given!' unless cql
        super(9, trace)
        @cql = cql
        @consistency = :one
      end

      def write(buffer, protocol_version, encoder)
        buffer.append_long_string(@cql)
      end

      def to_s
        %(PREPARE "#@cql")
      end

      def eql?(rq)
        self.class === rq && rq.cql == self.cql
      end
      alias_method :==, :eql?

      def hash
        @h ||= @cql.hash
      end
    end
  end
end
