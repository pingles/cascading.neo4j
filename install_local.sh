#!/usr/bin/env bash
rm -rf ~/.m2/repository/org/pingles/cascading.neo4j/
mvn install -Dmaven.test.skip=true