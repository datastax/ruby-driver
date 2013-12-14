# Contributing

## Report the bugs you find

All code have bugs, but if you report them they can be squashed.

The best bug reports include everything that is needed to reliably reproduce the bug. Try write a test case and include it in your report. Have a look at the [regression test suite](spec/integration/regression_spec.rb)) if you need inspiration.

If it's not possible to write a test case, for example because the bug only happens in very particular circumstances, or is not deterministic, make sure you include as much information as you can about the situation. The version of cql-rb is an absolute must, the version of Ruby and Cassandra are also very important. If there is a stack trace from the error make sure to include that (unfortunately the asynchronous nature of cql-rb means that the stack traces are not always as revealing as they could be).

At all times remember that the maintainers work on this project in their free time and that they don't work for you, or for your benefit. They have no obligation to drop everything to help you -- but if you're nice and make it easy for them they might do that anyway.

## Contribute new features

Fork the repository, make your changes in a topic branch that branches off from the right place in the history (`master` isn't necessarily always the right choice), make your changes and finally submit a pull request.

Follow the style of the existing code, make sure that existing tests pass, and that everything new has good test coverage. Put some effort into writing clear and concise commit messages, and write a good pull request description.

It takes time to understand other people's code, and even more time to understand a patch, so do as much as you can to make the maintainers' work easier. Be prepared for rejection, many times a feature is already planned, or the proposed design would be in the way of other planned features, or the maintainers just feel that it will be faster to implement the features themselves than to try to integrate your patch.

Feel free to open a pull request before the feature is finished, that way you can have a conversation with the maintainers during the development, and you can make adjustments to the design as you go along instead of having your whole feature rejected because of reasons such as those above. If you do, please make it clear that the pull request is a work in progress, or a request for comment.

Always remember that the maintainers work on this project in their free time and that they don't work for you, or for your benefit. They have no obligation to do what you think is right -- but if you're nice and make it easy for them they might do that anyway.
