const ADMIN_GRAPH = 'http://mu.semte.ch/graphs/organizations/kanselarij';
const MINISTER_GRAPH = 'http://mu.semte.ch/graphs/organizations/minister';
const CABINET_GRAPH = 'http://mu.semte.ch/graphs/organizations/intern-regering';
const GOVERNMENT_GRAPH = 'http://mu.semte.ch/graphs/organizations/intern-overheid';

const AGENDA_TYPE = 'https://data.vlaanderen.be/ns/besluitvorming#Agenda';
const DESIGN_AGENDA_STATUS = 'http://themis.vlaanderen.be/id/concept/agenda-status/b3d8a99b-0a7e-419e-8474-4b508fa7ab91';

const ACCESS_LEVEL_SECRETARY = 'http://themis.vlaanderen.be/id/concept/toegangsniveau/66804c35-4652-4ff4-b927-16982a3b6de8'; // intern secretarie
const ACCESS_LEVEL_CABINET = 'http://themis.vlaanderen.be/id/concept/toegangsniveau/13ae94b0-6188-49df-8ecd-4c4a17511d6d'; // intern regering
const ACCESS_LEVEL_GOVERNMENT = 'http://themis.vlaanderen.be/id/concept/toegangsniveau/634f438e-0d62-4ae4-923a-b63460f6bc46'; // intern overheid
const ACCESS_LEVEL_PUBLIC = 'http://themis.vlaanderen.be/id/concept/toegangsniveau/c3de9c70-391e-4031-a85e-4b03433d6266';

const DECISION_STATUS_APPROVED = 'http://themis.vlaanderen.be/id/concept/beslissing-resultaatcodes/56312c4b-9d2a-4735-b0b1-2ff14bb524fd';

export {
  ADMIN_GRAPH,
  MINISTER_GRAPH,
  CABINET_GRAPH,
  GOVERNMENT_GRAPH,
  AGENDA_TYPE,
  DESIGN_AGENDA_STATUS,
  ACCESS_LEVEL_SECRETARY,
  ACCESS_LEVEL_CABINET,
  ACCESS_LEVEL_GOVERNMENT,
  ACCESS_LEVEL_PUBLIC,
  DECISION_STATUS_APPROVED
}
