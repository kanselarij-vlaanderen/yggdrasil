import mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import { parseSparQlResults, logStage } from './helpers';
mu.query = querySudo;
import moment from 'moment';

let unconfidentialClassesCache = null;

const unconfidentialClasses = async function(queryEnv){
  if(unconfidentialClassesCache){
    return unconfidentialClassesCache;
  }
  const query = `
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  SELECT DISTINCT ?class WHERE {
    GRAPH <${queryEnv.targetGraph}> {
      ?class a ext:PublicClass .
    } 
  }`;
  let results = await queryEnv.run(query);
  results = parseSparQlResults(JSON.parse(results)).map((result) => {
    return result.class;
  });
  unconfidentialClassesCache = results;
  return unconfidentialClassesCache;
};

const fillUp = async function(queryEnv, extraFilter){
  const start = moment().utc();
  logStage(start, `fill public started at: ${start.format()}`, queryEnv.targetGraph);

  extraFilter = extraFilter || "";
  const classes = await unconfidentialClasses(queryEnv);

  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  INSERT {
    GRAPH <${queryEnv.targetGraph}> {
      ?s ?p ?o .
    }
  } WHERE {
    GRAPH <${queryEnv.adminGraph}> {
      VALUES ?class {
        <${classes.join('> <')}>
      }
      ?s a ?class .
      ?s ?p ?o .
      ${extraFilter}
    }
  }`;
  const result = await queryEnv.run(query);
  const end = moment().utc();
  logStage(start, `fill public ended at: ${end.format()}`, queryEnv.targetGraph);
  return result;
};

export {
  fillUp
};