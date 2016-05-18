# -*- encoding : utf-8 -*-

unless defined?(Rails)
 task :environment do
  end
end

namespace :cequel do
  namespace :keyspace do
    desc 'Initialize Cassandra keyspace'
    task :create => :environment do
      create!
    end

    desc 'Initialize Cassandra keyspace if not exist'
    task :create_if_not_exist => :environment do
      if Cequel::Record.connection.schema.exists?
        puts "Keyspace #{Cequel::Record.connection.name} already exists. Nothing to do."
        next
      end
      create!
    end

    desc 'Drop Cassandra keyspace'
    task :drop => :environment do
      drop!
    end

    desc 'Drop Cassandra keyspace if exist'
    task :drop_if_exist => :environment do
      unless Cequel::Record.connection.schema.exists?
        puts "Keyspace #{Cequel::Record.connection.name} doesn't exist. Nothing to do."
        next
      end
      drop!
    end
  end

  desc "Synchronize all models defined in `app/models' with Cassandra " \
       "database schema"
  task :migrate => :environment do
    migrate
  end

  desc "Create keyspace and tables for all defined models"
  task :init => %w(keyspace:create migrate)


  desc 'Drop keyspace if exists, then create and migrate'
  task :reset => :environment do
    if Cequel::Record.connection.schema.exists?
      drop!
    end
    create!
    migrate
  end

  def create!
    Cequel::Record.connection.schema.create!
    puts "Created keyspace #{Cequel::Record.connection.name}"
  end


  def drop!
    Cequel::Record.connection.schema.drop!
    puts "Dropped keyspace #{Cequel::Record.connection.name}"
  end

  def migrate
    models = Dir['app/models/**/*.rb'].reject{|f| f.start_with? "app/models/concern"}.map do |f| 
      f.gsub("app/models","").gsub(".rb","").camelize.constantize
    end
    cequel_models = models.select {|m| m.ancestors.include? Cequel::Record }.uniq 
    cequel_models.each do |model|
      model.synchronize_schema
      puts "Synchronized schema for #{model}"
    end
  end
end
