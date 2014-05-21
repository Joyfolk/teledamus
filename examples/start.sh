#!/bin/sh
rebar clean compile
erl -pa ebin edit deps/*/ebin -config examples.config -sname teledamus_examples -s examples 