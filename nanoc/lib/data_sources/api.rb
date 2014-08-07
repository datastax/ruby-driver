# encoding: utf-8

require 'yard'

module Docs
  class APIDataSource < Nanoc::DataSource
    identifier :api

    def up
      YARD::Templates::Engine.register_template_path templates_dir_name
    end

    def lib_dir_name
      config.fetch(:lib_dir, 'lib').chomp('/')
    end

    def templates_dir_name
      config.fetch(:template_paths, 'templates')
    end

    def items
      YARD::Registry.clear
      YARD.parse(lib_dir_name + '/**/*.rb')

      YARD::Verifier.new('!object.tag(:private) && (object.namespace.is_a?(CodeObjects::Proxy) || !object.namespace.tag(:private))').run(YARD::Registry.all(:module, :class)).map do |code|
        identifier = 'api/' + code.title.gsub(/([a-z])([A-Z])/, '\1_\2').downcase.gsub('::', '/')

        Nanoc::Item.new('', {:title => code.name, :code => code}, identifier)
      end
    end
  end
end
