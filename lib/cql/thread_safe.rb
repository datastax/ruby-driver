# encoding: utf-8

module Cql
  class ThreadSafe < ::BasicObject
    kernel = ::Kernel.dup
    kernel.class_eval do
      [:to_s,:inspect,:=~,:!~,:===,:<=>,:eql?,:hash].each do |m|
        undef_method m
      end
    end

    include kernel, ::MonitorMixin

    def self.const_missing(n)
      ::Object.const_get(n)
    end

    def initialize(object)
      @object = object

      mon_initialize
    end

    def method_missing(m, *args, &block)
      synchronize do
        begin
          @object.respond_to?(m) ? @object.__send__(m, *args, &block) : super(m, *args, &block)
        ensure
          $@.delete_if {|t| %r"\A#{Regexp.quote(__FILE__)}:#{__LINE__-2}:"o =~ t} if $@
        end
      end
    end
  end
end
