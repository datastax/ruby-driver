# encoding: utf-8

require 'rouge'

class CQL < Rouge::RegexLexer
  desc "Cassandra Query Language"
  tag 'cql'
  filenames '*.cql'
  mimetypes 'text/x-cql'

  def self.keywords
    @keywords ||= Set.new %w(
      ADD ALL ALTER AND ANY APPLY AS ASC AUTHORIZE BATCH BEGIN BY CLUSTERING
      COLUMNFAMILY COMPACT CONSISTENCY CONTAINS COUNT CREATE CUSTOM DELETE DESC
      DROP DISTINCT EXISTS FROM GRANT IF IN INDEX INSERT INTO KEY KEYSPACE
      LEVEL LIMIT MODIFY NORECURSIVE NOSUPERUSER NOT OF ON ORDER PERMISSION
      PERMISSIONS PRIMARY REVOKE SCHEMA SELECT STATIC STORAGE SUPERUSER TABLE
      TOKEN TRIGGER TRUNCATE TTL TYPE UPDATE USE USER USERS USING VALUES WHERE
      WITH WRITETIME ASCII BIGINT BLOB BOOLEAN COUNTER DECIMAL DOUBLE FLOAT
      INET INT TEXT TIMESTAMP TIMEUUID UUID VARCHAR VARINT LIST SET MAP
      REPLICATION DURABLE_WRITES TRUE FALSE NULL NAN INFINITY
    )
  end


  state :root do
    rule /\s+/m, Text
    rule /--.*?\n/, Comment::Single
    rule %r(/\*), Comment::Multiline, :multiline_comments
    rule /\d+/, Num::Integer
    rule %r{'}, Str::Single, :single_string
    rule %r{"}, Name::Variable, :double_string

    rule /\w[\w\d]*/ do |m|
      if self.class.keywords.include? m[0].upcase
        token Keyword
      else
        token Name
      end
    end

    rule %r([+*/<>=~!@#%^&|?^-]), Operator
    rule /[;:()\[\],.{}]/, Punctuation
  end

  state :multiline_comments do
    rule %r(/[*]), Comment::Multiline, :multiline_comments
    rule %r([*]/), Comment::Multiline, :pop!
    rule %r([^/*]+), Comment::Multiline
    rule %r([/*]), Comment::Multiline
  end

  state :single_string do
    rule /\\./, Str::Escape
    rule /''/, Str::Escape
    rule %r{'}, Str::Single, :pop!
    rule %r{[^\\']+}, Str::Single
  end

  state :double_string do
    rule /\\./, Str::Escape
    rule /""/, Str::Escape
    rule %r{"}, Name::Variable, :pop!
    rule %r{[^\\"]+}, Name::Variable
  end
end
