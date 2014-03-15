# encoding: utf-8

module Cql
  module Protocol
    class RawRowsResultResponse < RowsResultResponse
      def initialize(protocol_version, raw_rows, paging_state, trace_id)
        super(nil, nil, paging_state, trace_id)
        @protocol_version = protocol_version
        @raw_rows = raw_rows
      end

      def materialize(metadata)
        @metadata = metadata
        @rows = RowsResultResponse.read_rows(@protocol_version, @raw_rows, @metadata)
      end

      def rows
        raise UnmaterializedRowsError, 'Not materialized!' unless @rows
        @rows
      end

      def to_s
        %(RESULT ROWS (raw))
      end
    end
  end
end
