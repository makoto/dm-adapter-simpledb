require 'rubygems'
# simplerdb = In memory SimpleDB clone.
# Once downloaded from http://rubyfurnace.com/gems/simplerdb and installed,
# Apply patch at below url to fix a few issues.
# http://gist.github.com/20134
require 'simplerdb/server'

require 'aws_sdb'

require 'pathname'
require Pathname(__FILE__).dirname.parent.expand_path + 'lib/simpledb_adapter'

access_key = ENV['AMAZON_ACCESS_KEY_ID'] || raise("Error:Setup AMAZON_ACCESS_KEY_ID at your environment variable")
secret_key = ENV['AMAZON_SECRET_ACCESS_KEY'] || raise("Error:Setup AMAZON_SECRET_ACCESS_KEY at your environment variable")

# Start up in memory simplerdb for testing. Port can be anything
port = 8087
@server || begin
  @server = SimplerDB::Server.new(port)
  @thread = Thread.new { @server.start }
end

DataMapper.setup(:default, {
  :adapter => 'simpledb',
  :access_key => access_key,
  :secret_key => secret_key,
  :domain => 'missionaries',
  :url => "http://localhost:#{port}"
})

uri = DataMapper.repository.adapter.uri

# Creating domain for each test
AwsSdb::Service.new(
  :access_key_id => uri[:access_key],
  :secret_access_key => uri[:secret_key],
  :url => uri[:url]
).create_domain(uri[:domain])

class Person
  include DataMapper::Resource
  
  property :id,         String, :key => true
  property :name,       String, :key => true
  property :age,        Integer
  property :wealth,     Float
  property :birthday,   Date
  property :created_at, DateTime
  
  belongs_to :company
end

class Company
  include DataMapper::Resource
  
  property :id,   String, :key => true
  property :name, String, :key => true
  
  has n, :people
end