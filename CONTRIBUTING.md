# Contributing

## Report the bugs you find

All code has bugs, but if you report them they can be squashed.

The best bug reports include everything that is needed to reliably reproduce the bug.

### Running the Test Suite

Try to write a test case and include it in your report (have a look at the
[regression test suite](spec/regressions) if you need inspiration).

1. Bundle with `bundle install`
1. Run the unit test suite with `rake rspec`
  * Using this rake task will install necessary ruby extensions as a prerequisite
  * For `bundle exec rspec` to be successful, run `bundle exec rake compile` once, beforehand

If it's not possible to write a test case, for example because the bug only happens in
very particular circumstances, or is not deterministic, please still report the bug!

### Opening a ticket

Submit defect reports to our [Jira](https://datastax-oss.atlassian.net/projects/RUBY/issues). Include:

* The `cassandra-driver` version (`bundle show cassandra-driver | sed 's/.*\///'`)
* The Ruby version (`ruby -v`)
* The Cassandra version (2nd line printed when running `cqlsh`)
* A stack trace from the error, if there is one

## Pull Requests

If you're able to fix a bug yourself, you can
[fork the repository](https://help.github.com/articles/fork-a-repo/) and submit a
[Pull Request](https://help.github.com/articles/using-pull-requests/) with the fix.

Please create a ticket in [Jira](https://datastax-oss.atlassian.net/projects/RUBY/issues)
first, and reference the ticket in your pull request description.

## Contribution License Agreement

To protect the community, all contributors are required to
[sign the DataStax Contribution License Agreement](http://cla.datastax.com/).
The process is completely electronic and should only take a few minutes.
