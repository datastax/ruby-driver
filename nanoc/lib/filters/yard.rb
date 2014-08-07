# encoding: utf-8

require 'base64'
require 'fileutils'

module Docs
  class YARDFilter < Nanoc::Filter
    identifier :yard

    type :text => :text

    def self.setup
      YARD::Templates::Template.extra_includes << Docs::Helpers::YARD
    end

    def run(contents, params = {})
      @item[:code].format(params)
    end
  end
end
