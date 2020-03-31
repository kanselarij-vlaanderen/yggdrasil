import mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import {configurableQuery} from './helpers';
mu.query = querySudo;
import moment from 'moment';
import { removeInfoNotInTemp, addRelatedFiles, cleanup, addVisibleNewsletterInfo,
  fillOutDetailsOnVisibleItems, addAllRelatedDocuments, generateTempGraph,
  addAllRelatedToAgenda, addRelatedToAgendaItemAndSubcase, runStage, addVisibleDecisions,
  notInternRegeringFilter, notInternOverheidFilter, notConfidentialFilter,
  logStage, cleanupBasedOnLineage, filterAgendaMustBeInSet, copyTempToTarget,
  addVisibleNotulen, transformFilter
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
      ?s ext:agendaNaam ?naam.
      ${extraFilter}
    }
  }`;
  return queryEnv.run(query, true);
};



export const fillUp = async (queryEnv, agendas, toFile = false) => {
  try{
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
      return addAllDecisions(queryEnv, additionalFilter);
    });
    await runStage('visible notulen added', queryEnv, () => {
      return addAllNotulen(queryEnv, additionalFilter);
    });
    await runStage('visible newsletter info added', queryEnv, () => {
      return addVisibleNewsletterInfo(queryEnv, additionalFilter);
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
      const queryString = `
PREFIX  besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>\n 
CONSTRUCT {?s ?p ?o} WHERE {
GRAPH <${queryEnv.tempGraph}> {
?s ?p ?o.
}
}
`;
      const result = await configurableQuery(queryString, true, {
          overrideFormHeaders : {
              'content-type': 'text/turtle',
              'format': 'text/turtle'
          }
      });
      return result;
    }
    else{
      await runStage('lineage updated', queryEnv, () => {
        return cleanupBasedOnLineage(queryEnv, agendas);
    });
     if (queryEnv.fullRebuild){
       await runStage('removed info not in temp', queryEnv, () => {
        return removeInfoNotInTemp(queryEnv);
       });
      }
    }

    await runStage('copy temp to target', queryEnv, () => {
      return copyTempToTarget(queryEnv);
    });
    await runStage('cleaned up', queryEnv, cleanup);
    const end = moment().utc();
    logStage(start, `fill regering ended at: ${end.format()}`, targetGraph);
  }catch (e) {
    logStage(moment(), `${e}\n${e.stack}`, queryEnv.targetGraph);
    try {
      await cleanup(queryEnv);
    }catch (e2) {
      console.log(e2);
    }
  }
};
