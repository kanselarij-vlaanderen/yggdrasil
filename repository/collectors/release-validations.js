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

export function documentsReleaseFilter(isEnabled) {
  if (isEnabled) {
    // NOTE: we need the ?internalDocumentPublicationActivityUsed adms:status <http://themis.vlaanderen.be/id/concept/vrijgave-status/5da73f0d-6605-493c-9c1c-0d3a71bf286a> ("Planning bevestigd"@nl)
    // to ensure we only filter on the confirmed publication activities, not the ones that were already executed, which would have status <http://themis.vlaanderen.be/id/concept/vrijgave-status/27bd25d1-72b4-49b2-a0ba-236ca28373e5> ("Vrijgegeven"@nl)
    // If this filter were absent, it could cause documents associated with subcases in existing cases with other subcases that were previously published to be released as well.
    return `
      ?agenda
        besluitvorming:isAgendaVoor
          / ^ext:internalDocumentPublicationActivityUsed ?internalDocumentPublicationActivityUsed .
        ?internalDocumentPublicationActivityUsed prov:startedAtTime ?documentsReleaseDate ;
          adms:status <http://themis.vlaanderen.be/id/concept/vrijgave-status/5da73f0d-6605-493c-9c1c-0d3a71bf286a> .
      `;
  } else {
    return '';
  }
}
