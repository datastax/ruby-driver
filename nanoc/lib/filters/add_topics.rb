# encoding: utf-8

module Docs
  class AddTopicsFilter < Nanoc::Filter
    identifier :add_topics

    def run(content, params={})
      return content if @item.children.empty?

      content  = "#{content}"
      content << '<h3>Topics</h3>'
      content << '<ul class="sub-topics">'
      content << subnav(@item)
      content << '</ul>'
    end
  end
end
