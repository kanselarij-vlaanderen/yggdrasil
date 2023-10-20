const prefixes = {
  'besluit': 'http://data.vlaanderen.be/ns/besluit#',
  'besluitvorming': 'https://data.vlaanderen.be/ns/besluitvorming#',
  'dct': 'http://purl.org/dc/terms/',
  'ext': 'http://mu.semte.ch/vocabularies/ext/',
  'dossier': 'https://data.vlaanderen.be/ns/dossier#',
  'prov': 'http://www.w3.org/ns/prov#',
  'sign': 'http://mu.semte.ch/vocabularies/ext/handtekenen/',
  'pub': 'http://mu.semte.ch/vocabularies/ext/publicatie/',
  'adms': 'http://www.w3.org/ns/adms#',
};

const typeUris = [
  { key: 'agenda', uri: 'besluitvorming:Agenda' },
  { key: 'meeting', uri: 'besluit:Vergaderactiviteit' },
  { key: 'internalDecisionPublicationActivity', uri: 'ext:InternalDecisionPublicationActivity' },
  { key: 'internalDocumentPublicationActivity', uri: 'ext:InternalDocumentPublicationActivity' },
  { key: 'agendaitem', uri: 'besluit:Agendapunt' },
  { key: 'agendaActivity', uri: 'besluitvorming:Agendering' },
  { key: 'agendaStatusActivity', uri: 'ext:AgendaStatusActivity' },
  { key: 'submissionActivity', uri: 'ext:Indieningsactiviteit' },
  { key: 'subcase', uri: 'dossier:Procedurestap' },
  { key: 'case', uri: 'dossier:Dossier' },
  { key: 'decisionmakingFlow', uri: 'besluitvorming:Besluitvormingsaangelegenheid' },
  { key: 'publicationFlow', uri: 'pub:Publicatieaangelegenheid' },
  { key: 'identification', uri: 'adms:Identifier' },
  { key: 'agendaitemTreatment', uri: 'besluit:BehandelingVanAgendapunt' },
  { key: 'decisionActivity', uri: 'besluitvorming:Beslissingsactiviteit' },
  { key: 'newsitem', uri: 'ext:Nieuwsbericht' },
  { key: 'piece', uri: 'dossier:Stuk' },
  { key: 'signedPiece', uri: 'dossier:Stuk' },
  { key: 'documentContainer', uri: 'dossier:Serie' }
];

// TODO refactor collectors to make use of this model configuration to construct query paths
const pathsFromAgenda = {
  meeting: [
    { predicate: 'besluitvorming:isAgendaVoor' },
    { predicate: '^besluitvorming:behandelt' }
  ],
  internalDecisionPublicationActivity: [
    { source: 'meeting', predicate: '^ext:internalDecisionPublicationActivityUsed' }
  ],
  internalDocumentPublicationActivity: [
    { source: 'meeting', predicate: '^ext:internalDocumentPublicationActivityUsed' }
  ],
  agendaitem: [
    { predicate: 'dct:hasPart' }
  ],
  agendaActivity: [
    { source: 'agendaitem', predicate: '^besluitvorming:genereertAgendapunt' }
  ],
  submissionActivity: [
    { source: 'agendaActivity', predicate: 'prov:wasInformedBy' }
  ],
  agendaStatusActivity: [
    { predicate: '^prov:used' }
  ],
  subcase: [
    { source: 'agendaActivity', predicate: 'besluitvorming:vindtPlaatsTijdens' }
  ],
  decisionmakingFlow: [
    { source: 'subcase', predicate: '^dossier:doorloopt' }
  ],
  case: [
    { source: 'decisionmakingFlow', predicate: '^dossier:Dossier.isNeerslagVan' }
  ],
  agendaitemTreatment: [
    { source: 'agendaitem', predicate: '^dct:subject' }
  ],
  decisionActivity: [
    { source: 'agendaitemTreatment', predicate: 'besluitvorming:heeftBeslissing' }
  ],
  newsitem: [
    { source: 'agendaitemTreatment', predicate: '^prov:wasDerivedFrom' }
  ],
  piece: [
    { source: 'agendaitem', predicate: 'besluitvorming:geagendeerdStuk' },
    { source: 'agendaitem', predicate: 'ext:bevatReedsBezorgdAgendapuntDocumentversie' },
    // { source: 'decisionActivity', predicate: 'prov:used' }, // see resource files for comments
    { source: 'decisionActivity', predicate: '^besluitvorming:beschrijft' },
    { source: 'newsitem', predicate: 'besluitvorming:heeftBijlage' },
    { source: 'submissionActivity', predicate: 'prov:generated' },
    { source: 'case', predicate: 'dossier:Dossier.bestaatUit' },
    { source: 'subcase', predicate: 'ext:bevatReedsBezorgdeDocumentversie' },
    { source: 'meeting', predicate: 'ext:zittingDocumentversie' },
    { source: 'meeting', predicate: 'dossier:genereert' },
    { source: 'meeting', predicate: 'besluitvorming:heeftNotulen' },
  ],
  signedPiece: [
    { source: 'piece', predicate: '^sign:ongetekendStuk' }
  ],
  documentContainer: [
    { source: 'piece', predicate: '^dossier:Collectie.bestaatUit' }
  ],
  publicationFlow: [
    { source: 'piece', predicate: '^pub:referentieDocument' }
  ],
  identification: [
    { source: 'publicationFlow', predicate: 'adms:identifier' }
  ],
};

export {
  prefixes,
  typeUris,
  pathsFromAgenda
}
