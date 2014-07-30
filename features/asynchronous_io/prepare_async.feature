@wip
Feature: Preparing statements asynchronously

  Session objects can be used to prepare a statement asynchronously using `Cql::Session#prepare_async` method.

  Background:
    Given a running cassandra cluster with a keyspace "simplex" and a table "songs"

