# Ruby Driver

Apache Cassandra is a distributed, fault tolerant high throughput nosql database.

Ruby Driver is a ruby client for Apache Cassanrda with cql, asynchronous io,
peer discovery and custom policies support. Full feature list can be found in
features documentation.

## Quick Start

```ruby
require 'cql'

cluster = Cql.cluster.build
session = cluster.connect(keyspace = 'system')

session.execute('SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies').each do |row|
  puts "The keyspace #{row['keyspace_name']} has a table called #{row['columnfamily_name']}"
end
```

## Installation

As a rubygem:

```console
gem install cql-rb
```

Or add the following to your `Gemfile`:

```ruby
gem 'cql-rb'
```

## Usage

Refer to features and api documentation for more details.

### Constructing a cluster

```ruby
require 'cql'

cluster = Cql.cluster.build
```

### Establishing a new session

```ruby
require 'cql'

cluster = #...
session = cluster.connect
```

### Executing queries

```ruby
require 'cql'

session = #...
results = session.execute('SELECT * FROM table')

puts "total #{results.size} rows fetched"

results.each_with_index do |row, i|
  puts "Row ##{i + 1}"
  columns.each do |column|
    puts "  #{column}: #{row[column]}"
  end
end
```

### Prepared statements

```ruby
require 'cql'

session   = #...
statement = session.prepare('SELECT * FROM table LIMIT ?')

[1, 2, 3].each do |n|
  puts "selecting #{n} row(s)..."
  r = session.execute(statement, n)
  puts "#{r.size} rows selected"
end
```
