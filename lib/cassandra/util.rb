# encoding: utf-8

#--
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
#++

module Cassandra
  # @private
  module Util extend self
    def encode_hash(hash, io = StringIO.new)
      first = true

      io.putc(CRL_OPN)
      hash.each do |k, v|
        if first
          first = false
        else
          io.print(COMMA)
        end

        encode_object(k, io)
        io.print(COLON)
        encode_object(v, io)
      end
      io.putc(CRL_CLS)

      io.string
    end

    def encode_set(set, io = StringIO.new)
      first = true

      io.putc(CRL_OPN)
      set.each do |object|
        if first
          first = false
        else
          io.print(COMMA)
        end

        encode_object(object, io)
      end
      io.putc(CRL_CLS)

      io.string
    end

    def encode_array(array, io = StringIO.new)
      first = true

      io.putc(SQR_OPN)
      array.each do |object|
        if first
          first = false
        else
          io.print(COMMA)
        end

        encode_object(object, io)
      end
      io.putc(SQR_CLS)

      io.string
    end

    def encode_string(string, io = StringIO.new)
      io.putc(QUOT)
      string.chars do |c|
        case c
        when QUOT then io.print(ESC_QUOT)
        else
          io.putc(c)
        end
      end
      io.putc(QUOT)

      io.string
    end

    def encode_object(object, io = StringIO.new)
      case object
      when ::Hash    then encode_hash(object, io)
      when ::Array   then encode_array(object, io)
      when ::Set     then encode_set(object, io)
      when ::String  then encode_string(object, io)
      when ::Time    then encode_timestamp(object, io)
      when ::Numeric then encode_number(object, io)
      when ::IPAddr  then encode_inet(object, io)
      when Uuid      then encode_uuid(object, io)
      when nil       then io.print(NULL_STR)
      when false     then io.print(FALSE_STR)
      when true      then io.print(TRUE_STR)
      else
        raise ::ArgumentError, "unsupported type: #{object.inspect}"
      end

      io.string
    end
    alias :encode :encode_object

    def encode_timestamp(time, io = StringIO.new)
      io.print(time.to_i)
      io.string
    end

    def encode_number(number, io = StringIO.new)
      io.print(number)
      io.string
    end

    def encode_uuid(uuid, io = StringIO.new)
      io.print(uuid)
      io.string
    end

    def encode_inet(inet, io = StringIO.new)
      io.putc(QUOT)
      io.print(inet)
      io.putc(QUOT)
      io.string
    end

    def escape_name(name)
      return name if name[LOWERCASE_REGEXP] == name
      DBL_QUOT + name + DBL_QUOT
    end

    # @private
    LOWERCASE_REGEXP = /[[:lower:]\_]*/
    # @private
    NULL_STR = 'null'.freeze
    # @private
    FALSE_STR = 'false'.freeze
    # @private
    TRUE_STR = 'true'.freeze
    # @private
    CRL_OPN = '{'.freeze
    # @private
    CRL_CLS = '}'.freeze
    # @private
    SQR_OPN = '['.freeze
    # @private
    SQR_CLS = ']'.freeze
    # @private
    COMMA = ', '.freeze
    # @private
    COLON = ' : '.freeze
    # @private
    QUOT = ?'.freeze
    # @private
    ESC_QUOT = "''".freeze
    # @private
    DBL_QUOT = ?".freeze
  end
end
