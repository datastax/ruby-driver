def init
  sections :header, [T('docstring')]
end

def format_args(object)
  return if object.parameters.nil?
  params = object.parameters
  if object.has_tag?(:yield) || object.has_tag?(:yieldparam)
    params.reject! do |param|
      param[0].to_s[0,1] == "&" &&
        !object.tags(:param).any? {|t| t.name == param[0][1..-1] }
    end
  end

  unless params.empty?
    args = params.map {|n, v| v ? "<var>#{h n}</var> = #{h v}" : "<var>" + n.to_s + "</var>" }.join(", ")
    "<big>(</big>#{args}<big>)</big>"
  else
    ""
  end
end
