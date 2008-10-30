require 'rubygems'
# simplerdb = In memory SimpleDB clone.
# Once downloaded from http://rubyfurnace.com/gems/simplerdb and installed,
# Apply patch at below url to fix a few issues.
# http://gist.github.com/20134
require 'simplerdb/server'

require 'aws_sdb'
require 'amazon_sdb'

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
  # :url => "http://sdb.amazonaws.com"
  :url => "http://localhost:#{port}"
})

uri = DataMapper.repository.adapter.uri

# Creating domain for each test

# AwsSdb::Service.new(
#   :access_key_id => uri[:access_key],
#   :secret_access_key => uri[:secret_key],
#   :url => uri[:url]
# ).create_domain(uri[:domain])

# amazon_sdb does not allow you to set url as config, hence needs to modify constant ;-(
Amazon::SDB::Base::BASE_PATH = uri[:url]
sdb_base = Amazon::SDB::Base.new(uri[:access_key], uri[:secret_key])
# sdb_base.delete_domain!(uri[:domain])
sdb_base.create_domain(uri[:domain])

class Person
  include DataMapper::Resource
  
  # Note converted everything to string for now.
  property :id,         String, :key => true
  property :name,       String, :key => true
  property :age,        Integer
  property :wealth,     String
  property :birthday,   Time
  property :created_at, Time
  
  # property :age,        Integer
  # property :wealth,     Float
  # property :birthday,   Date
  # property :created_at, DateTime
  
  belongs_to :company
end

class Company
  include DataMapper::Resource
  
  property :id,   String, :key => true
  property :name, String, :key => true
  
  has n, :people
end