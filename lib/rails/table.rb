# frozen_string_literal: true
require 'pg'

module Rails
    class Table
      def initialize(adapter: {
        host: 'localhost',
        user: 'postgres',
        password: 'password',
        port: 5432
      }, options: {} )
        #raise ArgumentError, 'you must configure adapter ' if adapter.db == 'default'
        @conn = PG.connect(**adapter)
        @options = options
      end

      def show_create_table(table_name)
        @conn.exec_params(sql_show_create_table, [table_name]) do |result|
          result.each do |row|
            puts row
          end
        end
      end

      def dt
        @conn.exec_params(sql_dt) do |result|
          result.each do |row|
            puts row
          end
        end
      end

      def sql(query)
        @conn.exec_params(query) do |result|
          result.each do |row|
            puts row
          end
        end
      end

      private

      def sql_dt
        query = <<~SQL
          SELECT
            schemaname || '.' || tablename as table_full_name,
            tablename,
            schemaname,
            tableowner
          FROM pg_catalog.pg_tables
          WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
          ORDER BY schemaname, tablename;
        SQL
      end

      def sql_show_create_table
        ddl_query = <<~SQL
          SELECT
            'CREATE TABLE ' || relname || E'\n(\n' ||
            array_to_string(
              array_agg(
                '    ' || column_name || ' ' ||  type || ' '|| not_null
              )
              , E',\n'
            ) || E'\n);\n'
          FROM (
            SELECT
              c.relname, a.attname AS column_name,
              pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
              case
                when a.attnotnull
                then 'NOT NULL'
                else 'NULL'
              end as not_null
            FROM pg_class c,
              pg_attribute a
            WHERE c.relname = $1
              AND a.attnum > 0
              AND a.attrelid = c.oid
            ORDER BY a.attnum
          ) as tabledefinition
          GROUP BY relname;
        SQL
      end
  end
end
