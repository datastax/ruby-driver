# encoding: utf-8

module Cql
  module Client
    # @private
    class NullLogger
      def close(*); end
      def debug(*); end
      def debug?; false end
      def error(*); end
      def error?; false end
      def fatal(*); end
      def fatal?; false end
      def info(*); end
      def info?; false end
      def unknown(*); end
      def warn(*); end
      def warn?; false end
    end
  end
end
