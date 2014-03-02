# encoding: utf-8

module Cql
  module Protocol
    class QueryRequest < Request
      attr_reader :cql, :values, :type_hints, :consistency, :serial_consistency, :page_size, :paging_state

      def initialize(cql, values, type_hints, consistency, serial_consistency=nil, page_size=nil, paging_state=nil, trace=false)
        raise ArgumentError, %(No CQL given!) unless cql
        raise ArgumentError, %(No such consistency: #{consistency.inspect}) if consistency.nil? || !CONSISTENCIES.include?(consistency)
        raise ArgumentError, %(No such consistency: #{serial_consistency.inspect}) unless serial_consistency.nil? || CONSISTENCIES.include?(serial_consistency)
        raise ArgumentError, %(Bound values and type hints must have the same number of elements (got #{values.size} values and #{type_hints.size} hints)) if values && type_hints && values.size != type_hints.size
        raise ArgumentError, %(Paging state given but no page size) if paging_state && !page_size
        super(7, trace)
        @cql = cql
        @values = values || EMPTY_LIST
        @type_hints = type_hints || EMPTY_LIST
        @encoded_values = self.class.encode_values(CqlByteBuffer.new, @values, @type_hints)
        @consistency = consistency
        @serial_consistency = serial_consistency
        @page_size = page_size
        @paging_state = paging_state
      end

      def write(protocol_version, buffer)
        buffer.append_long_string(@cql)
        buffer.append_consistency(@consistency)
        if protocol_version > 1
          flags  = 0
          flags |= 0x04 if @page_size
          flags |= 0x08 if @paging_state
          flags |= 0x10 if @serial_consistency
          if @values && @values.size > 0
            flags |= 0x01
            buffer.append(flags.chr)
            buffer.append(@encoded_values)
          else
            buffer.append(flags.chr)
          end
          buffer.append_int(@page_size) if @page_size
          buffer.append_bytes(@paging_state) if @paging_state
          buffer.append_consistency(@serial_consistency) if @serial_consistency
        end
        buffer
      end

      def to_s
        %(QUERY "#@cql" #{@consistency.to_s.upcase})
      end

      def eql?(rq)
        self.class === rq &&
          rq.cql == self.cql &&
          rq.values == self.values &&
          rq.type_hints == self.type_hints &&
          rq.consistency == self.consistency &&
          rq.serial_consistency == self.serial_consistency &&
          rq.page_size == self.page_size &&
          rq.paging_state == self.paging_state
      end
      alias_method :==, :eql?

      def hash
        h = 0xcbf29ce484222325
        h = 0xffffffffffffffff & (0x100000001b3 * (h ^ @cql.hash))
        h = 0xffffffffffffffff & (0x100000001b3 * (h ^ @values.hash))
        h = 0xffffffffffffffff & (0x100000001b3 * (h ^ @type_hints.hash))
        h = 0xffffffffffffffff & (0x100000001b3 * (h ^ @consistency.hash))
        h = 0xffffffffffffffff & (0x100000001b3 * (h ^ @serial_consistency.hash))
        h = 0xffffffffffffffff & (0x100000001b3 * (h ^ @page_size.hash))
        h = 0xffffffffffffffff & (0x100000001b3 * (h ^ @paging_state.hash))
        h
      end

      def self.encode_values(buffer, values, hints)
        if values && values.size > 0
          buffer.append_short(values.size)
          values.each_with_index do |value, index|
            type = (hints && hints[index]) || guess_type(value)
            raise EncodingError, "Could not guess a suitable type for #{value.inspect}" unless type
            TYPE_CONVERTER.to_bytes(buffer, type, value)
          end
          buffer
        else
          buffer.append_short(0)
        end
      end

      private

      def self.guess_type(value)
        type = TYPE_GUESSES[value.class]
        if type == :map
          pair = value.first
          [type, guess_type(pair[0]), guess_type(pair[1])]
        elsif type == :list
          [type, guess_type(value.first)]
        elsif type == :set
          [type, guess_type(value.first)]
        else
          type
        end
      end

      TYPE_GUESSES = {
        String => :varchar,
        Fixnum => :bigint,
        Float => :double,
        Bignum => :varint,
        BigDecimal => :decimal,
        TrueClass => :boolean,
        FalseClass => :boolean,
        NilClass => :bigint,
        Uuid => :uuid,
        TimeUuid => :uuid,
        IPAddr => :inet,
        Time => :timestamp,
        Hash => :map,
        Array => :list,
        Set => :set,
      }.freeze
      TYPE_CONVERTER = TypeConverter.new
      EMPTY_LIST = [].freeze
      NO_FLAGS = "\x00".freeze
    end
  end
end
