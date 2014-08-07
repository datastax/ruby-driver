include T('docs/module/html')

def init
  super
  sections.place(:subclasses).before(:method_list)
end

def subclasses
  return if object.path == "Object" # don't show subclasses for Object
  unless globals.subclasses
    globals.subclasses = {}
    list = run_verifier Registry.all(:class)
    list.each do |o|
      (globals.subclasses[o.superclass.path] ||= []) << o if o.superclass
    end
  end

  @subclasses = globals.subclasses[object.path]
  return if @subclasses.nil? || @subclasses.empty?
  @subclasses = @subclasses.sort_by {|o| o.path }.map do |child|
    name = child.path
    if object.namespace
      name = object.relative_path(child)
    end
    [name, child]
  end
  erb(:subclasses)
end
