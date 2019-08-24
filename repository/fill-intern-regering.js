import mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
mu.query = querySudo;
import moment from 'moment';
import { removeInfoNotInTemp, addRelatedFiles, cleanup,
  fillOutDetailsOnVisibleItems, addAllRelatedDocuments,
  addAllRelatedToAgenda, addRelatedToAgendaItemAndSubcase,
  notBeperktOpenbaarFilter, notInternOverheidFilter, notConfidentialFilter
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
    }
  } WHERE {
    GRAPH <${queryEnv.adminGraph}> {
      ?s a <http://data.vlaanderen.be/ns/besluitvorming#Agenda>.
      ?s ext:agendaNaam ?naam.
      FILTER(?naam != "Ontwerpagenda")
      
      ${extraFilter}
    }
  }`;
  return queryEnv.run(query);
};

export const fillUp = async (queryEnv, agendaFilter) => {
  try{
    const start = moment().utc();
    console.log(`fill regering started at: ${start}`);
    await addVisibleAgendas(queryEnv, agendaFilter);
    logStage('agendas added', targetGraph);
    await addAllRelatedToAgenda(queryEnv, notConfidentialFilter);
    logStage('related to agenda added', targetGraph);
    await addRelatedToAgendaItemAndSubcase(queryEnv, notConfidentialFilter);
    logStage('agendaitem and subcase added', targetGraph);
    await addAllRelatedDocuments(queryEnv, notConfidentialFilter);
    logStage('related documents added', targetGraph);
    await addRelatedFiles(queryEnv);
    logStage('related files added', targetGraph);
    await fillOutDetailsOnVisibleItems(queryEnv);
    logStage('details added', targetGraph);
    await removeThingsWithLineageNoLongerInTemp(queryEnv);
    logStage('lineage updated', targetGraph);
    if(queryEnv.fullRebuild){
      await removeInfoNotInTemp(queryEnv);
      logStage('removed info not in temp', targetGraph);
    }
    await cleanup(queryEnv);
    logStage('cleaned up', targetGraph);
    const end = moment().utc();
    console.log(`fill regering ended at: ${end}, took: ${end.diff(start, 'ms')}ms`);
  }catch (e) {
    console.log(e);
  }
};