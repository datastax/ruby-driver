# Contributing

## Report the bugs you find

All code have bugs, but if you report them they can be squashed.

The best bug reports include everything that is needed to reliably reproduce the bug. Try write a test case and include it in your report (have a look at the [regression test suite](spec/integration/regression_spec.rb) if you need inspiration).

If it's not possible to write a test case, for example because the bug only happens in very particular circumstances, or is not deterministic, make sure you include as much information as you can about the situation. The version of the ruby driver is an absolute must, the version of Ruby and Cassandra are also very important. If there is a stack trace from the error make sure to include that (unfortunately the asynchronous nature of the ruby driver means that the stack traces are not always as revealing as they could be).

##Pull Requests

If you're able to fix a bug yourself, you can [fork the repository](https://help.github.com/articles/fork-a-repo/) and submit a [Pull Request](https://help.github.com/articles/using-pull-requests/) with the fix.
