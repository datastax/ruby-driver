# encoding: utf-8

Given(/^the following example running in the background:$/) do |code|
  step 'a file named "background_example.rb" with:', prepend_encoding(code)
  @background_process = run('ruby -I. -rbundler/setup background_example.rb', 5)
end

Then(/^background output should contain:$/) do |expected|
  sleep(5)
  if @background_process
    @background_process.terminate
    @background_output = @background_process.output
    @background_process = nil
  end

  assert_partial_output(expected, @background_output)
end
