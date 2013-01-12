# encoding: utf-8

module Cql
  EncodingError = Class.new(CqlError)

  module Encoding
    extend self

    def write_int(buffer, n)
      raise NotImplementedError
    end

    def write_short(buffer, n)
      buffer << [n].pack('n')
    end

    def write_string(buffer, str)
      buffer << [str.length].pack('n')
      buffer << str
      buffer
    end

    def write_long_string(buffer, str)
      buffer << [str.length].pack('N')
      buffer << str
      buffer
    end

    def write_uuid(buffer, uuid)
      raise NotImplementedError
    end

    def write_string_list(buffer, strs)
      buffer << [strs.size].pack('n')
      strs.each do |str|
        write_string(buffer, str)
      end
      buffer
    end

    def write_bytes(buffer, bytes)
      raise NotImplementedError
    end

    def write_short_bytes(buffer, bytes)
      raise NotImplementedError
    end

    def write_option(buffer, option)
      raise NotImplementedError
    end

    def write_option_list(buffer, options)
      raise NotImplementedError
    end

    def write_inet(buffer, ip, port)
      raise NotImplementedError
    end

    def write_consistency(buffer, consistency)
      index = CONSISTENCIES.index(consistency)
      raise EncodingError, %(Unknown consistency "#{consistency}") unless index
      write_short(buffer, index)
    end

    def write_string_map(buffer, map)
      buffer << [map.size].pack('n')
      map.each do |key, value|
        write_string(buffer, key)
        write_string(buffer, value)
      end
      buffer
    end

    def write_string_multimap(buffer, map)
      raise NotImplementedError
    end
  end
end