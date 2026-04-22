import {
  uuid as generateUuid,
  sparqlEscapeUri,
  sparqlEscapeString,
  sparqlEscapeDateTime,
} from "mu";
import { updateSudo } from "./auth-sudo";
import { JOB } from "../constants";

async function createDistributorJobs(deltaDistributors, agendaUris) {

  for (let distributor of deltaDistributors) {
    console.log('creating distributor jobs');
    const job = await createJob(distributor.targetGraph, agendaUris);
    distributor.jobUri = job.uri;
  }
}

async function createJob(distributorGraph, agendaUris) {
  const uuid = generateUuid();
  const job = {
    uri: JOB.RESOURCE_BASE + `/${JOB.JSONAPI_JOB_TYPE}/${uuid}`,
    id: uuid,
    status: JOB.STATUSES.SCHEDULED,
    created: new Date(),
  };
  const queryString = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX cogs: <http://vocab.deri.ie/cogs#>
  PREFIX adms: <http://www.w3.org/ns/adms#>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

  INSERT DATA {
  GRAPH <http://mu.semte.ch/graphs/organizations/kanselarij> {
      ${sparqlEscapeUri(job.uri)} a ${sparqlEscapeUri(JOB.RDF_TYPE)} ; # also a cogs:Job in spirit , not sure we want it
        mu:uuid ${sparqlEscapeString(job.id)} ;
        adms:status ${sparqlEscapeUri(job.status)} ;
        ${agendaUris.length ? `prov:used ${agendaUris.map(sparqlEscapeUri).join(", ")} ;` : ""}
        ext:targetGraph ${sparqlEscapeString(distributorGraph)} ;
        dct:created ${sparqlEscapeDateTime(job.created)} .
    }
  }`;
  await updateSudo(queryString);
  return job;
}

async function updateJobStatus(uri, status, errorMessage = '') {
  const time = new Date();
  let timePred;
  if (status === JOB.STATUSES.SUCCESS || status === JOB.STATUSES.FAILED) {
    // final statusses
    timePred = "http://www.w3.org/ns/prov#endedAtTime";
  } else {
    timePred = "http://www.w3.org/ns/prov#startedAtTime";
  }
  const escapedUri = sparqlEscapeUri(uri);
  const queryString = `
  PREFIX cogs: <http://vocab.deri.ie/cogs#>
  PREFIX adms: <http://www.w3.org/ns/adms#>
  PREFIX schema: <http://schema.org/>

  DELETE {
      GRAPH <http://mu.semte.ch/graphs/organizations/kanselarij> {
          ${escapedUri} adms:status ?status ;
              ${sparqlEscapeUri(timePred)} ?time .
      }
  }
  INSERT {
      GRAPH <http://mu.semte.ch/graphs/organizations/kanselarij> {
          ${escapedUri} adms:status ${sparqlEscapeUri(status)} .
          ${
            status !== JOB.STATUSES.SCHEDULED
              ? `${escapedUri} ${sparqlEscapeUri(timePred)} ${sparqlEscapeDateTime(time)} .`
              : ""
          }
          ${
            errorMessage
              ? `${escapedUri} schema:error ${sparqlEscapeString(errorMessage)} .`
              : ""
          }
      }
  }
  WHERE {
      GRAPH <http://mu.semte.ch/graphs/organizations/kanselarij> {
          ${escapedUri} a ${sparqlEscapeUri(JOB.RDF_TYPE)} .
          OPTIONAL { ${escapedUri} adms:status ?status }
          OPTIONAL { ${escapedUri} ${sparqlEscapeUri(timePred)} ?time }
      }
  }`;
  await updateSudo(queryString);
}

async function failUnfinishedDistributorJobs(errorMessage) {
  const queryString = `
  PREFIX cogs: <http://vocab.deri.ie/cogs#>
  PREFIX adms: <http://www.w3.org/ns/adms#>
  PREFIX schema: <http://schema.org/>
  PREFIX prov: <http://www.w3.org/ns/prov#>

  DELETE {
      GRAPH <http://mu.semte.ch/graphs/organizations/kanselarij> {
          ?distributionJob adms:status ?status ;
              prov:endedAtTime ?time .
      }
  }
  INSERT {
      GRAPH <http://mu.semte.ch/graphs/organizations/kanselarij> {
          ?distributionJob adms:status ${sparqlEscapeUri(JOB.STATUSES.FAILED)} .
          ?distributionJob prov:endedAtTime ${sparqlEscapeDateTime(new Date())} .
          ?distributionJob schema:error ${sparqlEscapeString(errorMessage)} .
      }
  }
  WHERE {
      GRAPH <http://mu.semte.ch/graphs/organizations/kanselarij> {
          ?distributionJob a ${sparqlEscapeUri(JOB.RDF_TYPE)} .
          ?distributionJob adms:status ?status .
          OPTIONAL { ?distributionJob prov:endedAtTime ?time }
          FILTER( ?status IN (<${JOB.STATUSES.SCHEDULED}>, <${JOB.STATUSES.BUSY}>) )
      }
  }`;
  await updateSudo(queryString);
}

export {
  createDistributorJobs,
  createJob,
  updateJobStatus,
  failUnfinishedDistributorJobs
};
