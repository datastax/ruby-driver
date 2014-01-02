# encoding: utf-8

module Cql
  module Protocol
    class RowsResultResponse < ResultResponse
      attr_reader :rows, :metadata, :paging_state

      def initialize(rows, metadata, paging_state, trace_id)
        super(trace_id)
        @rows, @metadata, @paging_state = rows, metadata, paging_state
      end

      def self.decode!(protocol_version, buffer, trace_id=nil)
        column_specs, paging_state = read_metadata!(protocol_version, buffer)
        if column_specs.nil?
          raise UnsupportedFeatureError, 'Cannot decode rows without column metadata'
        end
        new(read_rows!(protocol_version, buffer, column_specs), column_specs, paging_state, trace_id)
      end

      def to_s
        %(RESULT ROWS #@metadata #@rows)
      end

      private

      RESULT_TYPES[0x02] = self

      COLUMN_TYPES = [
        nil,
        :ascii,
        :bigint,
        :blob,
        :boolean,
        :counter,
        :decimal,
        :double,
        :float,
        :int,
        :text,
        :timestamp,
        :uuid,
        :varchar,
        :varint,
        :timeuuid,
        :inet,
      ].freeze

      GLOBAL_TABLES_SPEC_FLAG = 0x01
      HAS_MORE_PAGES_FLAG = 0x02
      NO_METADATA_FLAG = 0x04

      def self.read_column_type!(buffer)
        id, type = read_option!(buffer) do |id, b|
          if id > 0 && id <= 0x10
            COLUMN_TYPES[id]
          elsif id == 0x20
            sub_type = read_column_type!(buffer)
            [:list, sub_type]
          elsif id == 0x21
            key_type = read_column_type!(buffer)
            value_type = read_column_type!(buffer)
            [:map, key_type, value_type]
          elsif id == 0x22
            sub_type = read_column_type!(buffer)
            [:set, sub_type]
          else
            raise UnsupportedColumnTypeError, %(Unsupported column type: #{id})
          end
        end
        type
      end

      def self.read_metadata!(protocol_version, buffer)
        flags = read_int!(buffer)
        columns_count = read_int!(buffer)
        paging_state = nil
        column_specs = nil
        if flags & HAS_MORE_PAGES_FLAG != 0
          paging_state = read_bytes!(buffer)
        end
        if flags & NO_METADATA_FLAG == 0
          if flags & GLOBAL_TABLES_SPEC_FLAG != 0
            global_keyspace_name = read_string!(buffer)
            global_table_name = read_string!(buffer)
          end
          column_specs = columns_count.times.map do
            if global_keyspace_name
              keyspace_name = global_keyspace_name
              table_name = global_table_name
            else
              keyspace_name = read_string!(buffer)
              table_name = read_string!(buffer)
            end
            column_name = read_string!(buffer)
            type = read_column_type!(buffer)
            [keyspace_name, table_name, column_name, type]
          end
        end
        [column_specs, paging_state]
      end

      def self.read_rows!(protocol_version, buffer, column_specs)
        type_converter = TypeConverter.new
        rows_count = read_int!(buffer)
        rows = []
        rows_count.times do |row_index|
          row = {}
          column_specs.each do |column_spec|
            row[column_spec[2]] = type_converter.from_bytes(buffer, column_spec[3])
          end
          rows << row
        end
        rows
      end
    end
  end
end
