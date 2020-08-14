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

const copyClassesBatched = async function(queryEnv, classes, extraFilter) {
  if (!classes || classes.length == 0) {
    console.log("all done updating classes in public graph");
    return;
  }

  let classesToDo = [];
  const batchSize = 5;
  if (classes.length > batchSize) {
    classesToDo = classes.splice(0, batchSize);
  }

  const query = `
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
  await queryEnv.run(query);
  return copyClassesBatched(classesToDo);
}

const fillUp = async function(queryEnv, extraFilter){
  const start = moment().utc();
  logStage(start, `fill public started at: ${start.format()}`, queryEnv.targetGraph);

  extraFilter = extraFilter || "";
  const classes = await unconfidentialClasses(queryEnv);
  await copyClassesBatched(queryEnv, classes, extraFilter);

  const end = moment().utc();
  logStage(start, `fill public ended at: ${end.format()}`, queryEnv.targetGraph);
  return;
};

export {
  fillUp
};