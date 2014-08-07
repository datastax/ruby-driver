module Docs
  module Helpers
    module Subnav
      def subnav(item)
        item[:nav].each_with_object('') do |child, content|
          if @item == child
            content << "<li class=\"active\"><a href=\"#{child.path}\">#{child[:title]}</a><ul class=\"nav nav-pills nav-stacked\">{{TOC}}</ul></li>"
          else
            content << "<li><a href=\"#{child.path}\">#{child[:title]}</a></li>"
          end
        end
      end
    end
  end
end

include Docs::Helpers::Subnav
