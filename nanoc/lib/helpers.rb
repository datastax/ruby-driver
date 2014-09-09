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

LICENSE ||= "--\n" + File.read(File.dirname(__FILE__) + '/../../LICENSE').strip + "\n++"