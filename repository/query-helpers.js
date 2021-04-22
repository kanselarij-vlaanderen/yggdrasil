import { querySudo as query } from './auth-sudo';
import { SELECT_PAGE_SIZE } from '../config';

/**
 * Convert results of select query to an array of objects.
 * @method parseResult
 * @return {Array}
 */
function parseResult(result) {
  const bindingKeys = result.head.vars;
  return result.results.bindings.map((row) => {
    const obj = {};
    bindingKeys.forEach((key) => {
      if (row[key]) {
        obj[key] = row[key].value;
      } else {
        obj[key] = null;
      }
    });
    return obj;
  });
};

async function countResources({ graph, type = null, lineage = null }) {
  const statements = [];
  if (type)
    statements.push(`?s a <${type}> .`);
  else
    statements.push(`?s a ?type .`);

  if (lineage)
    statements.push(`?s ext:tracesLineageTo <${lineage}> .`);

  const queryResult = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT (COUNT(DISTINCT ?s) as ?count)
    WHERE {
      GRAPH <${graph}> {
        ${statements.join('\n')}
      }
    }
  `);

  return parseInt(queryResult.results.bindings[0].count.value);
}

async function getPagedResourceUris({ graph, type = null, lineage = null, offset = 0, limit = 10000 }) {
  const statements = [];
  if (type)
    statements.push(`?s a <${type}> .`);
  else
    statements.push(`?s a ?type .`);

  if (lineage)
    statements.push(`?s ext:tracesLineageTo <${lineage}> .`);

  const queryResult = await query(`
    SELECT DISTINCT ?s {
      GRAPH <${graph}> {
        ${statements.join('\n')}
      }
    }
    ORDER BY ?s
    LIMIT ${limit} OFFSET ${offset}
  `);

  return queryResult.results.bindings.map(b => b['s'].value);
}

async function getResourceUris({ graph, type = null, lineage = null }) {
  const count = await countResources({ graph, type, lineage });

  const resourceSet = new Set();
  if (count > 0) {
    const limit = SELECT_PAGE_SIZE;
    const totalBatches = Math.ceil(count / limit);
    let currentBatch = 0;

    while (currentBatch < totalBatches) {
      const offset = limit * currentBatch;
      const resourceUris = await getPagedResourceUris({ graph, type, lineage, limit, offset });
      resourceUris.forEach(resource => resourceSet.add(resource));
      currentBatch++;
    }
  }

  return [...resourceSet];
}

async function copyResource(subject, source, target) {
  await query(`
    INSERT {
      GRAPH <${target}> {
        <${subject}> ?p ?o .
      }
    } WHERE {
      GRAPH <${source}> {
        <${subject}> ?p ?o .
      }
    }
  `);
}

export {
  parseResult,
  countResources,
  getPagedResourceUris,
  getResourceUris,
  copyResource
}
