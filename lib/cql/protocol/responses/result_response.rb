# encoding: utf-8

module Cql
  module Protocol
    class ResultResponse < Response
      def self.decode!(buffer)
        kind = read_int!(buffer)
        case kind
        when 0x01
          VoidResultResponse.decode!(buffer)
        when 0x02
          RowsResultResponse.decode!(buffer)
        when 0x03
          SetKeyspaceResultResponse.decode!(buffer)
        when 0x04
          PreparedResultResponse.decode!(buffer)
        when 0x05
          SchemaChangeResultResponse.decode!(buffer)
        else
          raise UnsupportedResultKindError, %(Unsupported result kind: #{kind})
        end
      end

      def void?
        false
      end
    end
  end
end
