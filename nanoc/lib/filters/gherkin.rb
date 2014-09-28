# encoding: utf-8

require 'cgi'
require 'base64'
require 'ditaarb'

module Docs
  class GherkinFilter < Nanoc::Filter
    identifier :gherkin

    type :text => :text

    def run(contents, params = {})
      PARSER.parse(contents)
    end

    private

    class HTMLRenderer
      def initialize
        @formatter = Markdown.new
        @markdown  = Redcarpet::Markdown.new(@formatter, {})
      end

      def start_background(background)
        # p ['background', background]
        output = "<h2>Background</h2>"
        output << @markdown.render(background.description)
        output << "<dl class=\"steps\">"
      end

      def end_background(background)
        "</dl>"
      end

      def start_feature(feature)
        output = "<div class=\"feature-section\">"
        feature.tags.each do |tag|
          next unless tag.name.start_with?('@cassandra-version-')
          next if tag.name == '@cassandra-version-specific'
          output << "<div class=\"pull-right\"><span class=\"label label-primary\">since cassadra v#{tag.name.sub('@cassandra-version-', '')}</span></div>"
        end
        output << "<h1>#{feature.name}"
        output << "</h1>"
        output << @markdown.render(feature.description)
      end

      def end_feature(feature)
        '</div>'
      end

      def start_scenario(scenario)
        output = ''
        scenario.tags.each do |tag|
          next unless tag.name.start_with?('@cassandra-version-')
          next if tag.name == '@cassandra-version-specific'
          output << "<div class=\"pull-right\"><span class=\"label label-primary\">since cassadra v#{tag.name.sub('@cassandra-version-', '')}</span></div>"
        end
        output << "<h2>#{scenario.name}"
        output << "</h2><dl class=\"steps\">"
      end

      def end_scenario(scenario)
        "</dl>"
      end

      def step(step)
        # p ['step', step]
        output = "<dt>#{step.keyword}</dt>"
        output << "<dd>#{step.name}"

        if step.doc_string
          output << @formatter.block_code(step.doc_string.value, step.doc_string.content_type)
        end

        output << "</dd>"
      end
    end

    class Parser
      class Feature
        attr_reader :children

        def initialize(feature)
          @feature  = feature
          @children = []
        end

        def render(renderer)
          output = renderer.start_feature(@feature)
          @children.each do |child|
            output << child.render(renderer)
          end
          output << renderer.end_feature(@feature)
        end
      end

      class Container
        attr_reader :feature, :steps

        def initialize(feature)
          @feature = feature
          @steps   = []
        end

        private

        def render_steps(renderer)
          @steps.each_with_object('') do |step, output|
            output << step.render(renderer)
          end
        end
      end

      class Background < Container
        def initialize(feature, background)
          @background = background
          super(feature)
        end

        def render(renderer)
          output  = renderer.start_background(@background)
          output << render_steps(renderer)
          output << renderer.end_background(@background)
        end
      end

      class Scenario < Container
        def initialize(feature, scenario)
          @scenario = scenario
          super(feature)
        end

        def render(renderer)
          output  = renderer.start_scenario(@scenario)
          output << render_steps(renderer)
          output << renderer.end_scenario(@scenario)
        end
      end

      class ScenarioOutline < Container
        attr_writer :examples

        def initialize(feature, scenario_outline)
          @scenario_outline = scenario_outline
          super(feature)
        end

        def render(renderer)
          output  = renderer.start_scenario_outline(@scenario_outline)
          output << render_steps(renderer)
          output << renderer.examples(@examples)
          output << renderer.end_scenario_outline(@scenario_outline)
        end
      end

      class Step
        def initialize(step)
          @step = step
        end

        def render(renderer)
          renderer.step(@step)
        end
      end

      def initialize(renderer)
        @renderer = renderer
        @gherkin  = Gherkin::Parser::Parser.new(self)
        @features = []
      end

      def parse(contents)
        @output = ''
        @gherkin.parse(contents, nil, 0)
        @output
      end

      def uri(uri)
        self
      end

      def feature(feature)
        @features << @feature = Feature.new(feature)

        self
      end

      def background(background)
        @feature.children << @container = Background.new(self, background)

        self
      end

      def scenario(scenario)
        @feature.children << @container = Scenario.new(self, scenario)

        self
      end

      def scenario_outline(scenario_outline)
        @feature.children << @container = ScenarioOutline.new(self, scenario_outline)

        self
      end

      def examples(examples)
        @container.examples = examples
      end

      def step(step)
        @container.steps << Step.new(step)
      end

      def match(match)
        # p ['match', match]
        exit 1
      end

      def result(result)
        # p ['result', result]
        exit 1
      end

      def eof
        @features.each do |feature|
          @output << feature.render(@renderer)
        end.clear
      end
    end

    class Markdown < Redcarpet::Render::SmartyHTML
      def initialize(*args)
        super
      end

      def block_code(code, language)
        case language
        when nil, ''
          "<pre><code>#{CGI.escapeHTML(code)}</code></pre>"
        when 'ditaa'
          data = Base64.encode64(Ditaa.render(code, :separation => false))
          "<img src=\"data:image/png;base64,#{data}\" alt=\"Text Diagram\" class=\"img-rounded img-thumbnail center-block ditaa\" />"
        else
          markup = ::Rouge.highlight(code, language, 'html')
          markup.sub!(/<pre><code class="highlight">/,'<pre class="highlight"><code class="' + language + '">')
          markup.sub!(/<\/code><\/pre>/,"</code></pre>")
          markup.strip!
          markup
        end
      rescue
        "<pre><code class=\"#{language}\">#{CGI.escapeHTML(code)}</code></pre>"
      end
    end

    PARSER = Parser.new(HTMLRenderer.new)
  end
end
