Given(/^it is running interactively$/) do
  step 'I run `ruby -I. -rbundler/setup example.rb` interactively'
end

When(/^I type "(.*?)" (\d+) times$/) do |input, count|
  count.to_i.times do
    step "I type \"#{input}\""
  end
end
