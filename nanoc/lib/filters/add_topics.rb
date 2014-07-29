# encoding: utf-8

module Docs
  class AddTopicsFilter < Nanoc::Filter
    identifier :add_topics

    def run(content, params={})
      content + topics_for(@item.children)
    end

    private

    def topics_for(items)
      return '' if items.empty?

      content  = '<h3>Topics</h3>'
      content << '<ul class="sub-topics">'
      items.each do |item|
        content << "<li><a href=\"#{item.identifier}\">#{item[:title]}</a></li>"
      end
      content << '</ul>'
    end
  end
end
