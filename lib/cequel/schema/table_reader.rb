# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # A TableReader will query Cassandra's internal representation of a table's
    # schema, and build a {Table} instance exposing an object representation of
    # that schema
    #
    class TableReader
      COMPOSITE_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.CompositeType\((.+)\)$/
      REVERSED_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.ReversedType\((.+)\)$/
      COLLECTION_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.(List|Set|Map)Type\((.+)\)$/

      # @return [Table] object representation of the table defined in the
      #   database
      attr_reader :table

      #
      # Read the schema defined in the database for a given table and return a
      # {Table} instance
      #
      # @param (see #initialize)
      # @return (see #read)
      #
      def self.read(keyspace, table_name)
        new(keyspace, table_name).read
      end

      #
      # @param keyspace [Metal::Keyspace] keyspace to read the table from
      # @param table_name [Symbol] name of the table to read
      # @private
      #
      def initialize(keyspace, table_name)
        @keyspace, @table_name = keyspace, table_name
        @table = Table.new(table_name.to_sym)
      end
      private_class_method(:new)

      #
      # Read table schema from the database
      #
      # @return [Table] object representation of table in the database, or
      #   `nil` if no table by given name exists
      #
      # @api private
      #
      def read
        if table_data.present?
          read_partition_keys
          read_clustering_columns
          read_data_columns
          # read_properties
          table
        end
      end

      protected

      attr_reader :keyspace, :table_name, :table

      private

      # XXX This gets a lot easier in Cassandra 2.0: all logical columns
      # (including keys) are returned from the `schema_columns` query, so
      # there's no need to jump through all these hoops to figure out what the
      # key columns look like.
      #
      # However, this approach works for both 1.2 and 2.0, so better to keep it
      # for now. It will be worth refactoring this code to take advantage of
      # 2.0's better interface in a future version of Cequel that targets 2.0+.
      def read_partition_keys
        partition_columns.each do |column|
          table.add_partition_key(column.name.to_sym, Type.lookup_cql(column.type.kind))
        end
      end

      # XXX See comment on {read_partition_keys}
      def read_clustering_columns
        cluster_columns.zip(clustering_order) do |column, order|
          table.add_clustering_column(
            column.name.to_sym,
            Type.lookup_cql(column.type.kind),
            order
          )
        end
      end

      def read_data_columns
        column_data.each do |column|
          if column.type.kind == :list || column.type.kind == :set
            read_collection_column column.name, column.type.kind, column.type.value_type.kind
          elsif column.type.kind == :map
            key_type, value_type = column.type.key_type.kind, column.type.value_type.kind
            read_collection_column column.name, column.type.kind, key_type, value_type
          else
            index = index_for(column.name)
            table.add_data_column(
              column.name.to_sym,
              Type.lookup_cql(column.type.kind),
              index: index.try(:name).try(:to_sym),
              order: column.order,
              static: column.static?,
              frozen: column.frozen?
            )
          end
        end
      end

      def read_collection_column(name, collection_type, *internal_types)
        types = internal_types
          .map { |internal| Type.lookup_cql(internal) }
        table.__send__("add_#{collection_type}", name.to_sym, *types)
      end

      def read_properties
        table_data.slice(*Table::STORAGE_PROPERTIES).each do |name, value|
          table.add_property(name, value)
        end
        compaction = JSON.parse(table_data['compaction_strategy_options'])
          .symbolize_keys
        compaction[:class] = table_data['compaction_strategy_class']
        table.add_property(:compaction, compaction)
        compression = JSON.parse(table_data['compression_parameters'])
        table.add_property(:compression, compression)
      end

      def parse_composite_types(type_string)
        if COMPOSITE_TYPE_PATTERN =~ type_string
          $1.split(',')
        end
      end

      def table_data
        return @table_data if defined? @table_data
        statement = if keyspace.release_version.starts_with? '3'
                      <<-CQL
                            SELECT * FROM system_schema.tables
                            WHERE keyspace_name = ? AND table_name = ?
                      CQL
                    else
                      <<-CQL
                        SELECT * FROM system.schema_columnfamilies
                        WHERE keyspace_name = ? AND columnfamily_name = ?
                      CQL
                    end
        table_query = keyspace.execute(statement, keyspace.name, table_name)
        @table_data = table_query.first.try(:to_hash)
      end

      def all_columns
        @all_columns ||=
          if table_data
            statement = if keyspace.release_version.starts_with? '3'
                          <<-CQL
                            SELECT * FROM system_schema.columns
                            WHERE keyspace_name = ? AND table_name = ?
                          CQL
                        else
                          <<-CQL
                            SELECT * FROM system.schema_columns
                            WHERE keyspace_name = ? AND columnfamily_name = ?
                          CQL
                        end
            column_query = keyspace.execute(statement, keyspace.name, table_name)
            column_query.map(&:to_hash)
          end
      end

      def compact_value
        @compact_value ||= all_columns.find do |column|
          column['type'] == 'compact_value'
        end || {}
      end

      def column_data
        @column_data ||= cassandra_table.columns
      end

      def partition_columns
        @partition_columns ||= cassandra_table.partition_key
      end

      def cluster_columns
        @cluster_columns ||= cassandra_table.clustering_columns
      end

      def clustering_order
        @clustering_order ||= cassandra_table.clustering_order
      end

      def indexes
        @indexes ||= cassandra_table.indexes
      end

      def index_for(target)
        indexes.select {|index| index.target == target}.first
      end

      def cassandra_table
        keyspace.table(table_name)
      end
    end
  end
end
