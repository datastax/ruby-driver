# encoding: utf-8

module Docs
  class FeaturesDataSrouce < Nanoc::DataSource
    identifier :features

    def features_dir_name
      config.fetch(:features_dir, 'features')
    end

    def items
      items = []

      glob  = '**/*.*'
      dir   = features_dir_name
      glob  = [dir, glob].join('/') unless dir.empty?

      Dir[glob].each do |path|
        *base, filename = path.split('/')
        *filename, ext  = filename.split('.')
        filename = filename.join('.')

        if filename == 'README'
          title      = base.last.split('_').map(&:capitalize).join(' ')
          identifier = base.join('/')
          type       = :section
        elsif ext == 'feature'
          title      = filename.split('_').map(&:capitalize).join(' ')
          identifier = (base << filename).join('/')
          type       = :feature
        else
          next
        end

        items << Nanoc::Item.new(File.read(path), {:title => title, :extension => ext, :type => type}, identifier)
      end

      items
    end
  end
end