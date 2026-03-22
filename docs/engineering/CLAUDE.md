We're building a postgres schema migration tool. The development philosophy is to keep all features as modular as possible.

We want to test each feature properly. The idea is not to have too many tests, but the tests we do have should be of very high quality and test the underlying intent of the feature properly. A lot of thought should be put into writing concise tests that capture major edge cases.

Make sure the code builds regularly.

**Never** update REAMDE.md or ARCHITECTURE.md these are for human editing only. You may update docs/PRODUCTION_READINESS_PLAN.md.

We made a major error in our last attempt to build this library and did made our own parser instead of using a robust library based on libpg_query or a well maintained language wrappers like https://github.com/pganalyze/pg_query_go. We MUST use this library for parsing, never use our own hand rolled logic.

Make sure to keep code modulear and easy to test and iterate on. The code structure should be easy to onboard new engineers into. It should follow idiomatic clean coding practices. Tests should be split up into small files as well as application code. All code should be organized into clear directories with a coherant overall architecture. As the code grows in complexity you should reassess the current file structure and adapt to keep things clean and maintainable.

It's extremely important that you use an alert emoji to highlight any part of your tasks that were not 100% completed. Too often in the past you have been known to mention issues in passing at the top of a long summary and then have a wall of text with green passing emojis showing how excellent all your work has been. This is not helpful because it hides the most important parts of your work which are the failing cases. It's totally ok when we have work that was not completed 100% as long as you make it super clear at the end of your summary what is incomplete. That will be super helpful.

A previous agent reported Integration tests couldn't be run due to a pg_query build issue on macOS (third-party library issue), but unit tests confirm the logic is correct. This is a CRITICAL issue and must be resolved before any other work is carried out.
