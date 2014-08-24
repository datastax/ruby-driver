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
  class Column
    class Index
      attr_reader :name, :custom_class_name

      def initialize(name, custom_class_name = nil)
        @name              = name
        @custom_class_name = custom_class_name
      end
    end

    attr_reader :name, :type, :order, :index, :static

    def initialize(name, type, order, index = nil, is_static = false)
      @name     = name
      @type     = type
      @order    = order
      @index    = index
      @static   = is_static
    end

    def static?
      @static
    end

    def to_cql
      type = case @type
      when Array
        type, *args = @type
        "#{type.to_s}<#{args.map(&:to_s).join(', ')}>"
      else
        @type.to_s
      end

      cql = "#{@name} #{type}"
      cql << ' static' if @static
      cql
    end

    def eql?(other)
      other.is_a?(Column) &&
        @name == other.name &&
        @type == other.type &&
        @order == other.order &&
        @index == other.index &&
        @static == other.static?
    end
    alias :== :eql?
  end
end
