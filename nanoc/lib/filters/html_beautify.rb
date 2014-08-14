# encoding: utf-8

module Docs
  class HTMLBeaufity < Nanoc::Filter
    identifier :beautify

    type :text => :text

    requires 'htmlbeautifier'

    def run(contents, params = {})
      out = ''
      HtmlBeautifier::Beautifier.new(out).scan(contents)
      out
    end
  end
end
