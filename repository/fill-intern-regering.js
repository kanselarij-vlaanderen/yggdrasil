import mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
mu.query = querySudo;
import moment from 'moment';
import { removeInfoNotInTemp, addRelatedFiles, cleanup,
  fillOutDetailsOnVisibleItems, addAllRelatedDocuments, generateTempGraph,
  addAllRelatedToAgenda, addRelatedToAgendaItemAndSubcase, runStage, addVisibleDecisions,
  notInternRegeringFilter, notInternOverheidFilter, notConfidentialFilter,
  logStage, cleanupBasedOnLineage, filterAgendaMustBeInSet, copyTempToTarget
} from './helpers';

const addVisibleAgendas = (queryEnv, extraFilter) => {
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
      FILTER(?naam != "Ontwerpagenda")
      
      ${extraFilter}
    }
  }`;
  return queryEnv.run(query, true);
};

export const fillUp = async (queryEnv, agendas) => {
  try{
    const start = moment().utc();
    await generateTempGraph(queryEnv);
    const agendaFilter = filterAgendaMustBeInSet(agendas);
    const targetGraph = queryEnv.targetGraph;
    console.log(`fill regering started at: ${start}`);
    await runStage(`agendas added`, queryEnv, () => {
      return addVisibleAgendas(queryEnv, agendaFilter);
    });
    await runStage('related to agenda added', queryEnv, () => {
      return addAllRelatedToAgenda(queryEnv, notConfidentialFilter);
    });
    await runStage('agendaitem and subcase added', queryEnv, () => {
      return addRelatedToAgendaItemAndSubcase(queryEnv, notConfidentialFilter);
    });
    await runStage('visible decisions added', queryEnv, () => {
      return addVisibleDecisions(queryEnv, notConfidentialFilter);
    });
    await runStage('related documents added', queryEnv, () => {
      return addAllRelatedDocuments(queryEnv, notConfidentialFilter);
    });
    await runStage('related files added', queryEnv, () => {
      return addRelatedFiles(queryEnv);
    });
    await runStage('details added', queryEnv, () => {
      return fillOutDetailsOnVisibleItems(queryEnv);
    });
    await runStage('lineage updated', queryEnv, () => {
      return cleanupBasedOnLineage(queryEnv, agendas);
    });
    if(queryEnv.fullRebuild){
      await runStage('removed info not in temp', queryEnv, () => {
        return removeInfoNotInTemp(queryEnv);
      });
    }
    await runStage('copy temp to target', queryEnv, () => {
      return copyTempToTarget(queryEnv);
    });
    await runStage('cleaned up', queryEnv, () => {
      return cleanup(queryEnv);
    });
    const end = moment().utc();
    logStage(start, `fill regering ended at: ${end}`, targetGraph);
  }catch (e) {
    console.log(e);
    try {
      cleanup(queryEnv);
    }catch (e2) {
      console.log(e2);
    }
  }
};