# encoding: utf-8

module Cql
  module Protocol
    class ResultResponse < Response
      def self.decode!(buffer)
        kind = read_int!(buffer)
        impl = RESULT_TYPES[kind]
        raise UnsupportedResultKindError, %(Unsupported result kind: #{kind}) unless impl
        impl.decode!(buffer)
      end

      def void?
        false
      end

      private

      RESULT_TYPES = [
        # populated by subclasses
      ]
    end
  end
end
