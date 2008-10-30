require 'rubygems'
require 'dm-core'
require 'aws_sdb'
require 'amazon_sdb'
require 'digest/sha1'

# At default, amazon_sdb returns attribute as array, as you can store 
# This will cause 'should update a record' spec fail, as person.age returns [25], instead of 25
# I tried to get rid of array, but doing it using the below will break other matcher specs, so 
# commented out for now.
#
# module DataMapper
#   module Types
#     class Integer < DataMapper::Type
#       primitive String
# 
#       def self.load(value, property)
#         value.first if (value.class == Array && value.size == 1)
#       end
#       
#       def self.dump(value, property)
#          value.to_i
#       end
#     end # class Integer
#   end # module Types
# end # module DataMapper

module DataMapper
  module Adapters
    class SimpleDBAdapter < AbstractAdapter

      def create(resources)
        created = 0
        resources.each do |resource|
          item_name  = item_name_for_resource(resource)
          sdb_type = simpledb_type(resource.model)
          attributes = resource.attributes.merge(:simpledb_type => sdb_type)
          sdb.put_attributes(item_name,  Amazon::SDB::Multimap.new(attributes))
          created += 1
        end
        created
      end
      
      def delete(query)
        deleted = 0
        item_name = item_name_for_query(query)
        sdb.delete_attributes(item_name)
        deleted += 1
        raise NotImplementedError.new('Only :eql on delete at the moment') if not_eql_query?(query)
        deleted
      end
      
      def read_many(query)
        sdb_type = simpledb_type(query.model)
        
        conditions = ["['simpledb_type' = '#{sdb_type}']"]
        if query.conditions.size > 0
          conditions += query.conditions.map do |condition|
            operator = case condition[0]
              when :eql then '='
              when :not then '!='
              when :gt  then '>'
              when :gte then '>='
              when :lt  then '<'
              when :lte then '<='
              else raise "Invalid query operator: #{operator.inspect}"
            end
            
            # Because simpledb does have only String type, number needs to be padded to fixed length.
            # amazon_sdb will handle padding number when storing numeric, but won't help you quering.
            # Hence, you have to convert by yourself.
            # eg:
            # ['simpledb_type' = 'people'] intersection ['age' = '00000000000000000000000000000025']
            value = condition[2]
            value = sprintf("%032d", value.to_s) if value.class == Fixnum
            
            "['#{condition[1].name.to_s}' #{operator} '#{value}']"
          end
        end

        # Amazon::SDB::ResultSet contains items if :load_attrs is set to true.
        # http://nytimes.rubyforge.org/amazon_sdb/classes/Amazon/SDB/ResultSet.html
        results = sdb.query(:expr => conditions.compact.join(' intersection '), :load_attrs => true)
          
        Collection.new(query) do |collection|
          begin
            results.each do |result|
              data = query.fields.map do |property|
                value = result[property.field.to_s]
                if value.size > 1
                  value.map {|v| property.typecast(v) }
                else
                  property.typecast(value[0])
                end
              end
              collection.load(data)
            end
          rescue Amazon::SDB::RecordNotFoundError
            nil
          end
        end
      end
      
      def read_one(query)
        item_name = item_name_for_query(query)
        begin
          data = sdb.get_attributes(item_name)     
          # Returning nil at get!(*key) will raise DataMapper::ObjectNotFoundError 
          # at http://github.com/sam/dm-core/tree/master/lib/dm-core/model.rb#L248
        rescue  Amazon::SDB::RecordNotFoundError
          return nil
        end  
        data = query.fields.map do |property|
          value = data[property.field.to_s]
          if value.size > 1
            value.map {|v| property.typecast(v) }
          else
            property.typecast(value[0])
          end
        end
        query.model.load(data, query)
      end

      def update(attributes, query)
        updated = 0
        item_name = item_name_for_query(query)
        attributes = attributes.to_a.map {|a| [a.first.name.to_s, a.last]}.to_hash

        #Delete old attribute before insert new one.
        old_item = sdb.get_attributes(item_name)
        old_attributes = old_item.attributes.to_h.reject{|k,v| !(attributes.keys.include?(k))}
        sdb.delete_attributes(item_name, Amazon::SDB::Multimap.new(old_attributes))

        sdb.put_attributes(item_name, Amazon::SDB::Multimap.new(attributes))
        updated += 1
        raise NotImplementedError.new('Only :eql on delete at the moment') if not_eql_query?(query)
        updated
      end
            
    private
      
      # Returns the domain for the model
      def domain
        @uri[:domain]
      end
      
      # Creates an item name for a query
      def item_name_for_query(query)
        sdb_type = simpledb_type(query.model)
        
        item_name = "#{sdb_type}+"
        keys = keys_for_model(query.model)
        conditions = query.conditions.sort {|a,b| a[1].name.to_s <=> b[1].name.to_s }
        item_name += conditions.map do |property|
          property[2].to_s
        end.join('-')
        Digest::SHA1.hexdigest(item_name)
      end
      
      # Creates an item name for a resource
      def item_name_for_resource(resource)
        sdb_type = simpledb_type(resource.model)
        
        item_name = "#{sdb_type}+"
        keys = keys_for_model(resource.model)
        item_name += keys.map do |property|
          resource.instance_variable_get(property.instance_variable_name)
        end.join('-')
        
        Digest::SHA1.hexdigest(item_name)
      end
      
      # Returns the keys for model sorted in alphabetical order
      def keys_for_model(model)
        model.key(self.name).sort {|a,b| a.name.to_s <=> b.name.to_s }
      end
      
      def not_eql_query?(query)
        # Curosity check to make sure we are only dealing with a delete
        conditions = query.conditions.map {|c| c[0] }.uniq
        selectors = [ :gt, :gte, :lt, :lte, :not, :like, :in ]
        return (selectors - conditions).size != selectors.size
      end
      
      # Returns an SimpleDB instance to work with
      def sdb
        # @sdb ||= AwsSdb::Service.new(
        #   :access_key_id => @uri[:access_key], 
        #   :secret_access_key => @uri[:secret_key],
        #   :url => @uri[:url]
        # )
        
        # NOTE: 
        # BASE_PATH is set at spec_helper.rb
        @sdb ||= Amazon::SDB::Base.new( @uri[:access_key], @uri[:secret_key]).domain(@uri[:domain])
        @sdb
      end
      
      # Returns a string so we know what type of 
      def simpledb_type(model)
        model.storage_name(model.repository.name)
      end
      
    end # class SimpleDBAdapter
    
    # Required naming scheme.
    SimpledbAdapter = SimpleDBAdapter
    
  end # module Adapters
end # module DataMapper