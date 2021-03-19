import httpContext from 'express-http-context';
import { SparqlClient } from 'sparql-client-2';
import {
  ADMIN_GRAPH,
  DIRECT_SPARQL_ENDPOINT,
  LOG_DIRECT_QUERIES,
  NB_OF_QUERY_RETRIES,
  RETRY_TIMEOUT_MS
} from '../config';

function triplestoreSparqlClient() {
  let options = {
    requestDefaults: {
      headers: {
      }
    }
  };

  if (httpContext.get('request')) {
    options.requestDefaults.headers['mu-session-id'] = httpContext.get('request').get('mu-session-id');
    options.requestDefaults.headers['mu-call-id'] = httpContext.get('request').get('mu-call-id');
  }

  return new SparqlClient(DIRECT_SPARQL_ENDPOINT, options);
}

async function queryTriplestore(queryString) {
  if (LOG_DIRECT_QUERIES)
    console.log(queryString);

  const client = triplestoreSparqlClient();
  return await executeQuery(client, queryString);
}

async function executeQuery(client, queryString, { retries = NB_OF_QUERY_RETRIES }) {
  try {
    const response = await client.query(queryString).executeRaw();

    function maybeParseJSON(body) {
      try {
        return JSON.parse(body);
      } catch (ex) { // Catch invalid JSON
        return null;
      }
    }

    return maybeParseJSON(response.body);
  } catch (ex) {
    const retriesLeft = retries - 1;
    if (retriesLeft > 0) {
      const current = NB_OF_QUERY_RETRIES - retriesLeft;
      const timeout = current * RETRY_TIMEOUT_MS; // TODO make logarithmic
      console.log(`Failed to execute query (attempt ${current} out of ${NB_OF_QUERY_RETRIES}). Will retry.`);
      return new Promise(function(resolve, reject) {
        setTimeout(() => {
          try {
            const result = executeQuery(client, queryString, { retries: retriesLeft });
            resolve(result);
          } catch (ex) {
            reject(ex);
          }
        }, timeout);
      });
    } else {
      console.log(`Max number of retries reached. Query failed.\n ${queryString}`);
      throw ex;
    }
  }
}

const updateTriplestore = queryTriplestore;

export {
  queryTriplestore,
  updateTriplestore
};
