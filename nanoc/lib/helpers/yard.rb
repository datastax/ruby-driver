module Docs
  module Helpers
    module YARD
      def htmlify_line(*args)
        "<div class='inline'>" + htmlify(*args) + "</div>"
      end

      def format_return_types(object)
        return unless object.has_tag?(:return) && object.tag(:return).types
        return if object.tag(:return).types.empty?
        format_types object.tag(:return).types, false
      end

      def link_object(obj, title = nil, anchor = nil, relative = true)
        return title if obj.nil?
        obj = ::YARD::Registry.resolve(object, obj, true, true) if obj.is_a?(String)
        if title
          title = title.to_s
        elsif object.is_a?(::YARD::CodeObjects::Base)
          # Check if we're linking to a class method in the current
          # object. If we are, create a title in the format of
          # "CurrentClass.method_name"
          if obj.is_a?(::YARD::CodeObjects::MethodObject) && obj.scope == :class && obj.parent == object
            title = h([object.name, obj.sep, obj.name].join)
          elsif obj.title != obj.path
            title = h(obj.title)
          else
            title = h(object.relative_path(obj))
          end
        else
          title = h(obj.to_s)
        end
        return title if obj.is_a?(::YARD::CodeObjects::Proxy)

        link = url_for(obj, anchor)
        link = link ? link_url(link, title, :title => h("#{obj.title} (#{obj.type})")) : title
        link
      end

      def url_for(obj, anchor = nil, relative = true)
        link = nil
        return link if obj.is_a?(::YARD::CodeObjects::Base) && run_verifier([obj]).empty?

        if obj.is_a?(::YARD::CodeObjects::Base) && !obj.is_a?(::YARD::CodeObjects::NamespaceObject)
          # If the obj is not a namespace obj make it the anchor.
          anchor, obj = obj, obj.namespace
        end

        objpath = serialized_path(obj)
        return link unless objpath

        link = objpath
        link + (anchor ? '#' + urlencode(anchor_for(anchor)) : '')
      end

      def serialized_path(object)
        return object if object.is_a?(String)

        identifier = '/api/' + object.title.gsub(/([a-z])([A-Z])/, '\1_\2').downcase.gsub('::', '/') + '/'
        item       = options.site_items.at(identifier)
        item && item.path
      end
    end
  end
end
