include Nanoc::Helpers::Breadcrumbs
include Nanoc::Helpers::Rendering

def child_of?(item, parent)
  if item.parent == parent
    true
  elsif item.parent.nil?
    false
  else
    child_of?(item.parent, parent)
  end
end

def lunr_index
  unless File.directory?('node_modules')
    npm_bin  = Cliver.detect!('npm', '~> 2.1')

    system(npm_bin, 'install')
  end

  node_bin = Cliver.detect!('node', '~> 0.10')

  items = []

  @items.each do |item|
    next unless ['feature', 'md', 'rb'].include?(item[:extension])

    items << {
      'title' => item[:title],
      'text'  => Nokogiri::HTML(item.raw_content).text,
      'path'  => item.path
    }
  end

  json = JSON.dump(items)

  IO.popen("#{node_bin} nanoc/create-index.js", "r+") do |node|
    node.puts json
    node.close_write
    node.gets
  end
end

def pages_json
  data = {}

  @items.each do |item|
    next unless ['feature', 'md', 'rb'].include?(item[:extension])

    data[item.path] = {
      'title'   => item[:title],
      'summary' => item[:summary],
      'path'    => item.path
    }
  end

  JSON.dump(data)
end

LICENSE ||= "--\n" + File.read(File.dirname(__FILE__) + '/../../LICENSE').strip + "\n++"