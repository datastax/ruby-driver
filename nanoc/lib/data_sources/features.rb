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
      attrs = {:extension => extension, :section => '/' + features_dir_name}
      glob  = ['**', '*.' + extension].join('/')
      glob  = [features_dir_name, glob].join('/') unless features_dir_name.empty?

      Dir[glob].each do |path|
        *base, filename = path.split('/')
        *filename, _ = filename.split('.')
        filename = filename.join('.')
        title    = filename.split('_').map(&:capitalize).join(' ')

        items << Nanoc::Item.new(File.read(path), attrs.merge(:title => title), (base << filename).join('/'))
      end
    end
  end
end