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

module Cql
  module Statements
    class Simple
      include Statement

      attr_reader :cql, :params

      def initialize(cql, *params)
        @cql    = cql
        @params = params
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @cql=#{@cql.inspect} @params=#{@params.inspect}>"
      end

      def eql?(other)
        return false unless other.is_a?(Simple)
        @cql == other.cql && @params == other.params
      end

      alias :== :eql?
    end
  end
end
