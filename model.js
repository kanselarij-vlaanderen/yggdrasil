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
  'generiek': 'https://data.vlaanderen.be/ns/generiek#',
  'person': 'http://www.w3.org/ns/person#',
  'schema': 'http://schema.org/',
  'nfo': 'http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#',
};

const typeUris = [
  { key: 'agenda', uri: 'besluitvorming:Agenda' },
  { key: 'meeting', uri: 'besluit:Vergaderactiviteit' },
  { key: 'internalDecisionPublicationActivity', uri: 'ext:InternalDecisionPublicationActivity' },
  { key: 'internalDocumentPublicationActivity', uri: 'ext:InternalDocumentPublicationActivity' },
  { key: 'themisPublicationActivity', uri: 'ext:ThemisPublicationActivity' },
  { key: 'agendaitem', uri: 'besluit:Agendapunt' },
  { key: 'agendaActivity', uri: 'besluitvorming:Agendering' },
  { key: 'agendaStatusActivity', uri: 'ext:AgendaStatusActivity' },
  { key: 'submissionActivity', uri: 'ext:Indieningsactiviteit' },
  { key: 'subcase', uri: 'dossier:Procedurestap' },
  { key: 'case', uri: 'dossier:Dossier' },
  { key: 'decisionmakingFlow', uri: 'besluitvorming:Besluitvormingsaangelegenheid' },
  { key: 'publicationFlow', uri: 'pub:Publicatieaangelegenheid' },
  { key: 'identification', uri: 'adms:Identifier' },
  { key: 'structuredIdentifier', uri: 'generiek:GestructureerdeIdentificator' },
  { key: 'contactPerson', uri: 'schema:ContactPoint' },
  { key: 'person', uri: 'person:Person' },
  { key: 'translationSubcase', uri: 'pub:VertalingProcedurestap' },
  { key: 'publicationSubcase', uri: 'pub:PublicatieProcedurestap' },
  { key: 'requestActivity', uri: 'pub:AanvraagActiviteit' },
  { key: 'translationActivity', uri: 'pub:VertaalActiviteit' },
  { key: 'proofingActivity', uri: 'pub:DrukproefActiviteit' },
  { key: 'publicationActivity', uri: 'pub:PublicatieActiviteit' },
  { key: 'agendaitemTreatment', uri: 'besluit:BehandelingVanAgendapunt' },
  { key: 'decisionActivity', uri: 'besluitvorming:Beslissingsactiviteit' },
  { key: 'newsitem', uri: 'ext:Nieuwsbericht' },
  { key: 'piece', uri: 'dossier:Stuk' },
  { key: 'signedPiece', uri: 'dossier:Stuk' },
  { key: 'signedPieceCopy', uri: 'dossier:Stuk' },
  { key: 'documentContainer', uri: 'dossier:Serie' },
  { key: 'file', uri: 'nfo:FileDataObject' }
];

const typesToIgnore = [
  'ext:TempGraph', // Internal Yggdrasil type
  'prov:Activity', // Subclasses are configured for distribution
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
  themisPublicationActivity: [
    { source: 'meeting', predicate: '^prov:used' }
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
    { source: 'subcase', predicate: 'ext:heeftBekrachtiging' },
    { source: 'meeting', predicate: 'ext:zittingDocumentversie' },
    { source: 'meeting', predicate: 'dossier:genereert' },
    { source: 'meeting', predicate: 'besluitvorming:heeftNotulen' },
  ],
  signedPiece: [
    { source: 'piece', predicate: '^sign:ongetekendStuk' }
  ],
  signedPieceCopy: [
    { source: 'piece', predicate: 'sign:getekendStukKopie' }
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
  structuredIdentifier: [
    { source: 'identification', predicate: 'generiek:gestructureerdeIdentificator' }
  ],
  contactPerson: [
    { source: 'publicationFlow', predicate: 'prov:qualifiedDelegation' }
  ],
  person: [
    { source: 'contactPerson', predicate: '^schema:contactPoint' }
  ],
  translationSubcase: [
    { source: 'publicationFlow', predicate: 'pub:doorlooptVertaling' }
  ],
  publicationSubcase: [
    { source: 'publicationFlow', predicate: 'pub:doorlooptPublicatie' }
  ],
  requestActivity: [
    { source: 'translationSubcase', predicate: '^pub:aanvraagVindtPlaatsTijdensVertaling' },
    { source: 'publicationSubcase', predicate: '^pub:aanvraagVindtPlaatsTijdensPublicatie' }
  ],
  translationActivity: [
    { source: 'translationSubcase', predicate: '^pub:vertalingVindtPlaatsTijdens' },
  ],
  proofingActivity: [
    { source: 'publicationSubcase', predicate: '^pub:drukproefVindtPlaatsTijdens' }
  ],
  publicationActivity: [
    { source: 'publicationSubcase', predicate: '^pub:publicatieVindtPlaatsTijdens' }
  ],
};

export {
  prefixes,
  typeUris,
  typesToIgnore,
  pathsFromAgenda
}
