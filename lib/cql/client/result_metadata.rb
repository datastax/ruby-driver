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
  module Client
    # A collection of metadata (keyspace, table, name and type) of a result set.
    #
    # @see Cql::Client::ColumnMetadata
    class ResultMetadata
      include Enumerable

      # @private
      def initialize(metadata)
        @metadata = metadata.each_with_object({}) { |m, h| h[m[2]] = ColumnMetadata.new(*m) }
      end

      # Returns the column metadata
      #
      # @return [ColumnMetadata] column_metadata the metadata for the column
      def [](column_name)
        @metadata[column_name]
      end

      # Iterates over the metadata for each column
      #
      # @yieldparam [ColumnMetadata] metadata the metadata for each column
      # @return [Enumerable<ColumnMetadata>]
      def each(&block)
        @metadata.each_value(&block)
      end
    end
  end
end