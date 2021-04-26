const ADMIN_GRAPH = 'http://mu.semte.ch/graphs/organizations/kanselarij';
const MINISTER_GRAPH = 'http://mu.semte.ch/graphs/organizations/minister';
const CABINET_GRAPH = 'http://mu.semte.ch/graphs/organizations/intern-regering';
const GOVERNMENT_GRAPH = 'http://mu.semte.ch/graphs/organizations/intern-overheid';
const PUBLIC_GRAPH = 'http://mu.semte.ch/graphs/public';

const DESIGN_AGENDA_STATUS = 'http://kanselarij.vo.data.gift/id/agendastatus/2735d084-63d1-499f-86f4-9b69eb33727f';
const AGENDAITEM_FORMALLY_OK_STATUS = 'http://kanselarij.vo.data.gift/id/concept/goedkeurings-statussen/CC12A7DB-A73A-4589-9D53-F3C2F4A40636';

const ACCESS_LEVEL_CABINET = 'http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/d335f7e3-aefd-4f93-81a2-1629c2edafa3'; // intern regering
const ACCESS_LEVEL_GOVERNMENT = 'http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/abe4c18d-13a9-45f0-8cdd-c493eabbbe29'; // intern overheid
const ACCESS_LEVEL_PUBLIC = 'http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/6ca49d86-d40f-46c9-bde3-a322aa7e5c8e';

const DECISION_STATUS_APPROVED = 'http://kanselarij.vo.data.gift/id/concept/beslissings-resultaat-codes/56312c4b-9d2a-4735-b0b1-2ff14bb524fd';

export {
  ADMIN_GRAPH,
  MINISTER_GRAPH,
  CABINET_GRAPH,
  GOVERNMENT_GRAPH,
  PUBLIC_GRAPH,
  DESIGN_AGENDA_STATUS,
  AGENDAITEM_FORMALLY_OK_STATUS,
  ACCESS_LEVEL_CABINET,
  ACCESS_LEVEL_GOVERNMENT,
  ACCESS_LEVEL_PUBLIC,
  DECISION_STATUS_APPROVED
}
