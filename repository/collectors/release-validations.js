import { sparqlEscapeUri } from "mu";
import { DECISION_STATUS_ACKNOWLEDGED, DECISION_STATUS_APPROVED } from "../../constants";
/**
 Filters to apply in a SPARQL query to validate on releases of specific items.
 `?agenda` is always used as hook to apply the filter on.

 Note: the filters always return a string value such that they can be used inline
 in a SPARQL query without the need for additional null-checking.
*/

export function decisionsReleaseFilter(isEnabled) {
  if (isEnabled) {
    return `
      ?agenda
        besluitvorming:isAgendaVoor
          / ^ext:internalDecisionPublicationActivityUsed
          / prov:startedAtTime
        ?decisionReleaseDate .`;
  } else {
    return '';
  }
}

export function documentsReleaseFilter(isEnabled, validateDecisionResults) {
  if (isEnabled) {
    // NOTE: we need the ?agenda dct:hasPart / besluitvorming:geagendeerdStuk ?piece to ensure we only propagate the documents for agendas that are relevant to these documents.
    // If this filter were absent, it could cause documents associated with subcases in existing cases with other subcases that were previously published to be propagated as well.
    // E.g., imagine creating a new subcase in an existing case, for which the earlier subcases were already released with an existing agenda at an earlier time.
    // Any pieces added to the new subcase are linked to the existing parent case as well, causing the document release query to return results with a ?documentsReleaseDate in the past.

    // NOTE: we also want to propagate documents only when they are approved in a ?decisionActivity. Documents that have only been retracted or postponed should not be propagated yet.
    // One of the issues is that legacy does not have ?decisionResult set. Also that ?decisionResult is not set upon creation of new data.
    // so the ?startDate of ?decisionActivity is used to ensure we only allow missing a result on legacy data. 
    // The other issue is that we want to propagate the piece when approved, but only the predicates that are connected to the agenda where they are released.
    // Older agendas where the postponed happened should still not have documents. Updating those older agendas should not "clean" the approved documents either
    // so we only propagate the relation  if the ?decisionActivity that has the approved /acknowledged status 
    return `
      ?agenda dct:hasPart / besluitvorming:geagendeerdStuk ?piece .
      ?agenda
        besluitvorming:isAgendaVoor
          / ^ext:internalDocumentPublicationActivityUsed
          / prov:startedAtTime
        ?documentsReleaseDate .
      ${validateDecisionResults ? `
      ?agenda dct:hasPart / ^dct:subject / besluitvorming:heeftBeslissing ?decisionActivity .
      ?decisionActivity ^besluitvorming:heeftBeslissing / dct:subject / besluitvorming:geagendeerdStuk ?piece .
      ?decisionActivity dossier:Activiteit.startdatum ?startDate .
      OPTIONAL { ?decisionActivity besluitvorming:resultaat ?decisionResult }
      FILTER ( (!BOUND(?decisionResult) && ?startDate < xsd:dateTime("2019-10-01T00:00:00+01:00")) ||
        ?decisionResult = ${sparqlEscapeUri(DECISION_STATUS_APPROVED)} ||
        ?decisionResult = ${sparqlEscapeUri(DECISION_STATUS_ACKNOWLEDGED)} 
      )
      ` : ''}
  `;
  } else {
    return '';
  }
}
