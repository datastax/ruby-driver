# encoding: utf-8

module Docs
  class YardDataSource < Nanoc::DataSource
    identifier :yard

    def lib_dir_name
      config.fetch(:lib_dir, 'lib').chomp('/')
    end

    def template_paths
      config.fetch(:template_paths, 'templates')
    end

    def up
      require 'yard'
      YARD.parse(lib_dir_name + '/**/*.rb')
      YARD::Templates::Engine.template_paths << template_paths
    end

    def items
      items = []

      add(items,     YARD::Registry.at('Cql'))
      add(items,     YARD::Registry.at('Cql::Builder'))
      add(items,     YARD::Registry.at('Cql::Cluster'))
      add(items,     YARD::Registry.at('Cql::Session'))
      add(items,     YARD::Registry.at('Cql::Future'))
      add(items,     YARD::Registry.at('Cql::Statement'))
      add_all(items, YARD::Registry.at('Cql::Statements'))
      add(items,     YARD::Registry.at('Cql::Host'))
      add_all(items, YARD::Registry.at('Cql::LoadBalancing'))
      add_all(items, YARD::Registry.at('Cql::Retry'))
      add_all(items, YARD::Registry.at('Cql::Reconnection'))
      add_all(items, YARD::Registry.at('Cql::Auth'))
      add_all(items, YARD::Registry.at('Cql::Execution'))
      # add(items,     YARD::Registry.at('Cql::Result'))
      add_all(items, YARD::Registry.at('Cql::Results'))

      items
    end

    private

    def add(items, code)
      identifier = lib_dir_name + '/' + code.title.split('::').join('/')
      items << Nanoc::Item.new(code.format(:format => :html, :template => :guide), {:title => code.name, :type => code.type}, identifier)
    end

    def add_all(items, code)
      add(items, code)
      code.children.each do |child|
        add(items, child) if child.type == :module || child.type == :class
      end
    end
  end
end
