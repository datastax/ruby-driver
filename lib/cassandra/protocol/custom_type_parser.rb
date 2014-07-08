# encoding: utf-8

module Cassandra
  module Protocol
    class CustomTypeParser
      def parse_type(str)
        parse_custom_type(str).first
      end

      private

      def parse_custom_type(str)
        open_parentheses_index = str.index('(')
        if open_parentheses_index
          type = str[0, open_parentheses_index]
          rest = str[open_parentheses_index + 1, str.bytesize - open_parentheses_index - 1]
          if type == USER_TYPE
            start_index = str.index(',', str.index(',') + 1)
            rest = str[start_index + 1, str.bytesize - start_index - 1]
            field_types, rest = parse_user_type_fields(rest)
            rest = rest && rest[0, rest.bytesize - 1]
            [[:udt, Hash[field_types]], rest]
          elsif type == LIST_TYPE
            type, rest = parse_custom_type(rest)
            rest = rest && rest[1, rest.bytesize - 1]
            [[:list, type], rest]
          elsif type == MAP_TYPE
            key_type, value_type, rest = parse_map_fields(rest)
            rest = rest && rest[1, rest.bytesize - 1]
            [[:map, key_type, value_type], rest]
          elsif type == SET_TYPE
            type, rest = parse_custom_type(rest)
            rest = rest && rest[1, rest.bytesize - 1]
            [[:set, type], rest]
          else
            [nil, rest]
          end
        else
          type = SCALAR_TYPES.keys.find do |type|
            str.start_with?(type)
          end
          if type
            rest = str[type.bytesize, str.bytesize - str.bytesize]
            [SCALAR_TYPES[type], rest]
          else
            [[:custom, str], nil]
          end
        end
      end

      def parse_user_type_fields(str)
        return [[], str] if str.nil? || str.empty?
        comma_index = str.index(',')
        open_parentheses_index = str.index('(')
        close_parentheses_index = str.index(')')
        if close_parentheses_index == 0
          return [[], str]
        elsif open_parentheses_index && (comma_index.nil? || comma_index > open_parentheses_index)
          end_index = open_parentheses_index
        elsif comma_index && (close_parentheses_index.nil? || close_parentheses_index > comma_index)
          end_index = comma_index
        else
          end_index = close_parentheses_index
        end
        field = str[0, end_index]
        name, type = field.split(':')
        name = [name].pack('H*')
        rest = str[end_index + 1, str.bytesize - end_index - 1]
        if end_index == open_parentheses_index
          colon_index = field.index(':')
          type, rest = parse_custom_type(str[colon_index + 1, str.bytesize - colon_index - 1])
          types, rest = parse_user_type_fields(rest)
        elsif end_index == comma_index
          types, rest = parse_user_type_fields(rest)
          type = SCALAR_TYPES[type]
        else
          type = SCALAR_TYPES[type]
        end
        [[[name, type], *types], rest]
      end

      def parse_map_fields(str)
        key_type, rest = parse_custom_type(str)
        rest = rest[1, rest.bytesize - 1]
        value_type, rest = parse_custom_type(rest)
        [key_type, value_type, rest]
      end

      LIST_TYPE = 'org.apache.cassandra.db.marshal.ListType'.freeze
      MAP_TYPE = 'org.apache.cassandra.db.marshal.MapType'.freeze
      SET_TYPE = 'org.apache.cassandra.db.marshal.SetType'.freeze
      USER_TYPE = 'org.apache.cassandra.db.marshal.UserType'.freeze

      SCALAR_TYPES = {
        'org.apache.cassandra.db.marshal.AsciiType' => :ascii,
        'org.apache.cassandra.db.marshal.BooleanType' => :boolean,
        'org.apache.cassandra.db.marshal.BytesType' => :bytes,
        'org.apache.cassandra.db.marshal.CounterColumnType' => :counter,
        'org.apache.cassandra.db.marshal.DateType' => :date,
        'org.apache.cassandra.db.marshal.DecimalType' => :decimal,
        'org.apache.cassandra.db.marshal.DoubleType' => :double,
        'org.apache.cassandra.db.marshal.FloatType' => :float,
        'org.apache.cassandra.db.marshal.InetAddressType' => :inet,
        'org.apache.cassandra.db.marshal.Int32Type' => :int,
        'org.apache.cassandra.db.marshal.IntegerType' => :int,
        'org.apache.cassandra.db.marshal.LongType' => :bigint,
        'org.apache.cassandra.db.marshal.TimeUUIDType' => :time_uuid,
        'org.apache.cassandra.db.marshal.TimestampType' => :timestamp,
        'org.apache.cassandra.db.marshal.UTF8Type' => :text,
        'org.apache.cassandra.db.marshal.UUIDType' => :uuid,
      }.freeze
    end
  end
end
