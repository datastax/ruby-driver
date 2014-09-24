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

When(/^it is executed with a valid ca path in the environment$/) do
  ENV['SERVER_CERT'] = @server_cert

  step 'it is executed'

  ENV.delete('SERVER_CERT')
end

When(/^it is executed with ca and cert path and key in the environment$/) do
  ENV['SERVER_CERT'] = @server_cert
  ENV['CLIENT_CERT'] = @client_cert
  ENV['PRIVATE_KEY'] = @private_key
  ENV['PASSPHRASE']  = @passphrase

  step 'it is executed'

  ENV.delete('SERVER_CERT')
  ENV.delete('CLIENT_CERT')
  ENV.delete('PRIVATE_KEY')
  ENV.delete('PASSPHRASE')
end
