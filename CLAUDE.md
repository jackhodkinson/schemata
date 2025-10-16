We're building a postgres schema migration tool. The development philosophy is to keep all features as modular as possible.

We want to test each feature properly. The idea is not to have too many tests, but the tests we do have should be of very high quality and test the underlying intent of the feature properly. A lot of thought should be put into writing concise tests that capture major edge cases.

Make sure the code builds regularly.

**Never** update REAMDE.md or ARCHITECTURE.md these are for human editing only. You may update PLAN.md.

We made a major error in our last attempt to build this library and did made our own parser instead of using a robust library based on libpg_query or a well maintained language wrappers like https://github.com/pganalyze/pg_query_go. We MUST use this library for parsing, never use our own hand rolled logic.

Make sure to keep code modulear and easy to test and iterate on. The code structure should be easy to onboard new engineers into. It should follow idiomatic clean coding practices. Tests should be split up into small files as well as application code. All code should be organized into clear directories with a coherant overall architecture. As the code grows in complexity you should reassess the current file structure and adapt to keep things clean and maintainable.
