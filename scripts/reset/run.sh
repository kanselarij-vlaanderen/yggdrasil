#!/bin/sh
echo ""
echo "Going to drop minister graph in triplestore"
curl -X DELETE --url "http://triplestore:8890/sparql-graph-crud?graph=http://mu.semte.ch/graphs/organizations/minister"

echo ""
echo "Going to drop intern-regering graph in triplestore"
curl -X DELETE --url "http://triplestore:8890/sparql-graph-crud?graph=http://mu.semte.ch/graphs/organizations/intern-regering"

echo ""
echo "Going to drop intern-overheid graph in triplestore"
curl -X DELETE --url "http://triplestore:8890/sparql-graph-crud?graph=http://mu.semte.ch/graphs/organizations/intern-overheid"

echo ""
echo "DONE"
