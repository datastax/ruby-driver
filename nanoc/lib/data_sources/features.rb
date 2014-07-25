# encoding: utf-8

module Docs
  class FeaturesDataSrouce < Nanoc::DataSource
    identifier :features

    def features_dir_name
      config.fetch(:features_dir, 'features')
    end

    def items
      items = []

      add_items_to(items, 'feature')
      add_items_to(items, 'md')

      items
    end

    private

    def add_items_to(items, extension)
      attrs = {:extension => extension}
      glob  = ['**', '*.' + extension].join('/')
      glob  = [features_dir_name, glob].join('/') unless features_dir_name.empty?

      Dir[glob].each do |path|
        items << Nanoc::Item.new(File.read(path), attrs, path.split('.').first)
      end
    end
  end
end