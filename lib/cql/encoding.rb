# encoding: utf-8

module Cql
  module Encoding
    extend self

    def write_int(buffer, n)
      raise NotImplementedError
    end

    def write_short(buffer, n)
      raise NotImplementedError
    end

    def write_string(buffer, str)
      buffer << [str.length].pack('n')
      buffer << str
      buffer
    end

    def write_long_string(buffer, str)
      raise NotImplementedError
    end

    def write_uuid(buffer, uuid)
      raise NotImplementedError
    end

    def write_string_list(buffer, strs)
      raise NotImplementedError
    end

    def write_bytes(buffer, bytes)
      raise NotImplementedError
    end

    def write_short_bytes(buffer, bytes)
      raise NotImplementedError
    end

    def write_long_string(buffer, str)
      raise NotImplementedError
    end

    def write_option(buffer, option)
      raise NotImplementedError
    end

    def write_option_list(buffer, options)
      raise NotImplementedError
    end

    def write_long_string(buffer, str)
      raise NotImplementedError
    end

    def write_inet(buffer, ip, port)
      raise NotImplementedError
    end

    def write_long_string(buffer, str)
      raise NotImplementedError
    end

    def write_consistency(buffer, consistency)
      raise NotImplementedError
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