require 'pp'
require 'sequel'
require 'inifile'
require "tiny_tds"
require 'date'

tables = ["table_name1", "table_name2]
puts "tables to sync...."
pp tables

# begin script time tracker
start = Time.now

# connect to postgresql
DB_PG = Sequel.connect(:adapter=>'postgresql', :host=>'host', :port=>5432, :database=>'database_name', :user=>'username', :timeout=>600000)

puts "Connected to postgres..."

# connect to SQL SERVER
database_name = 'database_name' 
	
DB_MS = Sequel.connect(
  adapter: 'tinytds', 
  host: host, 
  database: database_name, 
  user: username, 
  password: password,
  timeout: 600000
)

# iterate through tables
tables.each do |table|

table_name = table
table_name = table_name.downcase
puts "syncing: #{table_name}..."


# delete existing entries in postgres table
DB_PG.run("delete from \"#{table_name}\"")

table_ms = DB_MS[table_name.to_sym]
table_pg = DB_PG[table_name.to_sym]

columns = table_ms.columns

column_count = columns.count

puts "columns"
pp columns

row_counter = 1

# iterate through each row in SQL SERVER database table
table_ms.each do |row| 
	# pp row
	# insert into postgres database
	values_query=''
	counter = 1
	
	# iterate through values of record; create sql insert query
	row.values.each do |v| 
		v_orig = v
		
		# convert to string
		v = v.to_s
		# escape single quotes in values by adding another quote
		v.gsub!("'", "''")

		# if string is '.'' make it 0
		if v == '.'
			v = '0'
		end

		# value is null if nil or empty string else surround in single quotes.
		if v == nil or v == ''
			v = "null"
		else
			v = "'" + v + "'"
		end

		values_query << v

		# Add comma and space unless it's the last value
		values_query << ', ' unless counter == column_count

		counter += 1
	end

	insert_query = "insert into \"#{table_name}\" values (#{values_query})"

	# pp insert_query
	DB_PG.run(insert_query)

	puts "row inserted: #{row_counter} in #{table_name}" if row_counter % 50 == 0 
	row_counter += 1

end # row iteration

end # table iteration

# Amount of time to run code
finish = Time.now

diff = ((finish - start)/60).round(2)
pp "Code Execution Time: #{diff} minutes"
