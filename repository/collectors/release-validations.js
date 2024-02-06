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

export function documentsReleaseFilter(isEnabled, noPostponedOrRetracted=false) {
  if (isEnabled) {
    // NOTE: we need the ?agenda dct:hasPart / besluitvorming:geagendeerdStuk ?piece to ensure we only propagate the documents for agendas that are relevant to these documents.
    // If this filter were absent, it could cause documents associated with subcases in existing cases with other subcases that were previously published to be propagated as well.
    // E.g., imagine creating a new subcase in an existing case, for which the earlier subcases were already released with an existing agenda at an earlier time.
    // Any pieces added to the new subcase are linked to the existing parent case as well, causing the document release query to return results with a ?documentsReleaseDate in the past.
    return `
      ?agenda dct:hasPart / besluitvorming:geagendeerdStuk ?piece .
      ?agenda
        besluitvorming:isAgendaVoor
          / ^ext:internalDocumentPublicationActivityUsed
          / prov:startedAtTime
        ?documentsReleaseDate .
      ${noPostponedOrRetracted ? `
        ?decisionActivity ^besluitvorming:heeftBeslissing / dct:subject / besluitvorming:geagendeerdStuk ?piece .
        { ?decisionActivity besluitvorming:resultaat <http://themis.vlaanderen.be/id/concept/beslissing-resultaatcodes/56312c4b-9d2a-4735-b0b1-2ff14bb524fd> } # goedgekeurd
        UNION
        { ?decisionActivity besluitvorming:resultaat <http://themis.vlaanderen.be/id/concept/beslissing-resultaatcodes/9f342a88-9485-4a83-87d9-245ed4b504bf> } # akte genomen
      ` : ''}
  `;
  } else {
    return '';
  }
}
