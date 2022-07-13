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
    return `
      ?agenda 
        besluitvorming:isAgendaVoor
          / ^ext:internalDocumentPublicationActivityUsed
          / prov:startedAtTime
        ?documentsReleaseDate .
      `;
  } else {
    return '';
  }
}
