import mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import {configurableQuery} from './helpers';
mu.query = querySudo;
import moment from 'moment';
import { removeInfoNotInTemp, addRelatedFiles, cleanup, addAllNewsletterInfo,
  fillOutDetailsOnVisibleItems, generateTempGraph, addAllRelatedDocuments,
  addRelatedToAgendaItemAndSubcase, runStage, addAllTreatments,
  logStage, cleanupBasedOnLineage, filterAgendaMustBeInSet, copyTempToTarget,
  transformFilter
} from './helpers';

const addAgendas = (queryEnv, extraFilter) => {
  extraFilter = extraFilter || "";

  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a <http://data.vlaanderen.be/ns/besluitvorming#Agenda>.
      ?s ext:tracesLineageTo ?s.
    }
  } WHERE {
    GRAPH <${queryEnv.adminGraph}> {
      ?s a <http://data.vlaanderen.be/ns/besluitvorming#Agenda>.
      
      ${extraFilter}
    }
  }`;
  return queryEnv.run(query, true);
};

const addAllRelatedToAgenda = function(queryEnv){
  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a ?thing .
      ?s ext:tracesLineageTo ?agenda .
    }
  } WHERE {
    GRAPH <${queryEnv.tempGraph}> {
      ?agenda a besluitvorming:Agenda .
    }
    GRAPH <${queryEnv.adminGraph}> {
      ?agenda ?p ?s .
      ?s a ?thing .
    }
  }`;
  return queryEnv.run(query, true);
};

const jsonToNtriples = function(json){
  const triples = JSON.parse(json).results.bindings.map((binding) => {
    let object = binding.o.value;
    if(binding.o.type == "uri"){
      object =`<${object}>`;
    }else{
      object = `"${object}"`;
    }
    if(binding.o.datatype){
      object = `${object}^^<${binding.o.datatype}>`;
    }

    return `<${binding.s.value}> <${binding.p.value}> ${object} . `;
  });
  return triples.join("\n")
};

const writeResultToFile = async function(queryEnv, start, anonymize=true){
  let queryString = `
PREFIX  besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>\n 
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
CONSTRUCT {?s ?p ?newo} WHERE {
  GRAPH <${queryEnv.tempGraph}> {
    ?s ?p ?o.
    BIND(IF(DATATYPE(?o) = xsd:string && ?p != <http://mu.semte.ch/vocabularies/core/uuid>, STR(?s), ?o) AS ?newo )
  }
}
`;

  if (!anonymize){
    queryString = `
PREFIX  besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>\n 
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
CONSTRUCT {?s ?p ?o} WHERE {
  GRAPH <${queryEnv.tempGraph}> {
    ?s ?p ?o.
  } 
}`
  }
  const result = await configurableQuery(queryString, true);
  await runStage('cleaned up', queryEnv, cleanup);
  const end = moment().utc();
  logStage(start, `fill kanselarij ended at: ${end.format()}`, queryEnv.targetGraph);
  // can't use ntriples directly as it does not utf8 encode
  return jsonToNtriples(result);
};



export const fillUp = async (queryEnv, agendas, options) => {
  try{
    const {toFile, anonymize} = options;
    const start = moment().utc();
    await generateTempGraph(queryEnv);
    const agendaFilter = filterAgendaMustBeInSet(agendas);
    const targetGraph = queryEnv.targetGraph;
    const additionalFilter = "";
    console.log(`fill kanselarij started at: ${start.format()}`);
    await runStage(`agendas added`, queryEnv, () => {
      return addAgendas(queryEnv, agendaFilter);
    });
    await runStage('related to agenda added', queryEnv, () => {
      return addAllRelatedToAgenda(queryEnv);
    });
    await runStage('agendaitem and subcase added', queryEnv, () => {
      return addRelatedToAgendaItemAndSubcase(queryEnv, additionalFilter);
    });
    await runStage('visible decisions added', queryEnv, () => {
      return addAllTreatments(queryEnv, additionalFilter);
    });
    await runStage('visible newsletter info added', queryEnv, () => {
      return addAllNewsletterInfo(queryEnv, additionalFilter);
    });
    await runStage('related documents added', queryEnv, () => {
      return addAllRelatedDocuments(queryEnv, '');
    });
    await runStage('related files added', queryEnv, () => {
      return addRelatedFiles(queryEnv, transformFilter(additionalFilter, "?docVersion", "?docVersion (ext:file | ext:convertedFile ) ?s ."));
    });
    await runStage('details added', queryEnv, () => {
      return fillOutDetailsOnVisibleItems(queryEnv);
    });

    if (toFile) {
      return writeResultToFile(queryEnv, start, anonymize);
    }

    await runStage('lineage updated', queryEnv, () => {
      return cleanupBasedOnLineage(queryEnv, agendas);
    });
    if (queryEnv.fullRebuild){
      await runStage('removed info not in temp', queryEnv, () => {
        return removeInfoNotInTemp(queryEnv);
      });
    }

    await runStage('copy temp to target', queryEnv, () => {
      return copyTempToTarget(queryEnv);
    });
    await runStage('cleaned up', queryEnv, cleanup);
    const end = moment().utc();
    logStage(start, `fill kanselarij ended at: ${end.format()}`, targetGraph);
  }catch (e) {
    logStage(moment(), `${e}\n${e.stack}`, queryEnv.targetGraph);
    try {
      await cleanup(queryEnv);
    }catch (e2) {
      console.log(e2);
    }
  }
};
