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
  module Statements
    class Prepared
      include Statement

      attr_reader :cql, :params_metadata, :result_metadata, :execution_info

      def initialize(cql, params_metadata, result_metadata, execution_info)
        @cql             = cql
        @params_metadata = params_metadata
        @result_metadata = result_metadata
        @execution_info  = execution_info
      end

      def bind(*args)
        raise ::ArgumentError, "expecting exactly #{@params_metadata.size} bind parameters, #{args.size} given" if args.size != @params_metadata.size

        Bound.new(@cql, @params_metadata, @result_metadata, args)
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @cql=#{@cql.inspect}>"
      end
    end
  end
end
