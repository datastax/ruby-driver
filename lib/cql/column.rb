# encoding: utf-8

module Cql
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
