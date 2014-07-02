# encoding: utf-8

When(/^it is executed with a valid username and password in the environment$/) do
  ENV['USERNAME'] = @username
  ENV['PASSWORD'] = @password

  step 'it is executed'

  ENV.delete('USERNAME')
  ENV.delete('PASSWORD')
end

When(/^it is executed with an invalid username and password in the environment$/) do
  ENV['USERNAME'] = 'invalidname'
  ENV['PASSWORD'] = 'badpassword'

  step 'it is executed'

  ENV.delete('USERNAME')
  ENV.delete('PASSWORD')
end
