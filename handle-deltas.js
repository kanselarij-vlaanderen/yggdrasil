import debounce from 'debounce';
import { ok } from 'assert';
import moment from 'moment';

import { parseSparQlResults, directQuery } from './repository/helpers';

const DEBUG = process.env.DEBUG === 'true';
const DELTA_TIMEOUT = parseInt(process.env.DELTA_TIMEOUT) || (5 * 60 * 1000);

let builders = {};

const handleDeltaRelatedToAgenda = async function(subjects, queryEnv) {
  if (DEBUG) {
    console.log(`Found subjects: ${JSON.stringify(subjects)}`);
  }
  const start = moment();
  const relatedAgendas = await selectRelatedAgendasForSubjects(subjects);
  if (DEBUG) {
    const diff = moment().diff(start, 'seconds', true).toFixed(3);
    console.log(`Related to subjects: ${relatedAgendas} -- ${diff}s`);
  }
  if (!relatedAgendas || !relatedAgendas.length) {
    return;
  }

  for (let builderName in builders) {
    let targetEnv = builders[builderName];
    await targetEnv.builder.fillUp(targetEnv.env, relatedAgendas);
  }
};

const pathsToAgenda = {
  'agendaitem': ['^dct:hasPart'],
  'agenda-activity' : ['besluitvorming:genereertAgendapunt / ^dct:hasPart'],
  'subcase': ['^besluitvorming:vindtPlaatsTijdens / besluitvorming:genereertAgendapunt / ^dct:hasPart'],
  'meeting': ['^besluitvorming:isAgendaVoor'],
  'newsletter-info': [
    { path: '^prov:generated', nextRDFType: 'subcase' },
    { path: '^ext:algemeneNieuwsbrief', nextRDFType: 'meeting' }
  ],
  'consultation-request': [
    { path: '^ext:bevatConsultatievraag', nextRDFType: 'subcase' }
  ],
  'treatment': [
    { path: 'besluitvorming:heeftOnderwerp', nextRDFType: 'agendaitem' },
  ],
  'meeting-record': [
    { path: '^ext:notulenVanAgendaPunt', nextRDFType: 'agendaitem' },
    { path: '^ext:algemeneNotulen', nextRDFType: 'meeting' }
  ],
  'case': [
    { path: 'dossier:doorloopt', nextRDFType: 'subcase' }
  ],
  'remark': [
    { path: '^ext:antwoorden* / ^besluitvorming:opmerking', nextRDFType: 'meeting' },
    { path: '^ext:antwoorden* / ^besluitvorming:opmerking', nextRDFType: 'newsletter-info' },
    { path: '^ext:antwoorden* / ^besluitvorming:opmerking', nextRDFType: 'document-container' },
    { path: '^ext:antwoorden* / ^besluitvorming:opmerking', nextRDFType: 'agendaitem' },
    { path: '^ext:antwoorden* / ^besluitvorming:opmerking', nextRDFType: 'case' },
    { path: '^ext:antwoorden* / ^besluitvorming:opmerking', nextRDFType: 'subcase' }
  ],
  'document-container': [
    { path: '^besluitvorming:genereertVerslag', nextRDFType: 'treatment' },
    { path: 'dossier:collectie.bestaatUit', nextRDFType: 'document' },
    { path: '^ext:getekendeNotulen', nextRDFType: 'meeting-record' }
  ],
  'document': [
    { path: '^ext:bevatDocumentversie', nextRDFType: 'subcase' },
    { path: '^ext:zittingDocumentversie', nextRDFType: 'meeting' },
    { path: '^besluitvorming:geagendeerdStuk', nextRDFType: 'agendaitem' },
    { path: '^ext:documentenVoorPublicatie', nextRDFType: 'newsletter-info' },
    //{ path: '^ext:documentenVoorBeslissing', nextRDFType: 'treatment' }, // Currently not used in the frontend.
    { path: '^ext:getekendeDocumentVersiesVoorNotulen', nextRDFType: 'meeting-record' }
  ]
};

const typeUris = {
  'agenda': 'besluitvorming:Agenda',
  'agendaitem': 'besluit:Agendapunt',
  'agenda-activity': 'besluitvorming:Agendering',
  'subcase': 'dbpedia:UnitOfWork',
  'meeting': 'besluit:Vergaderactiviteit',
  'newsletter-info': 'besluitvorming:NieuwsbriefInfo',
  'consultation-request': 'besluitvorming:Consultatievraag',
  'treatment': 'besluit:BehandelingVanAgendapunt',
  'meeting-record': 'ext:Notule',
  'case': 'dossier:Dossier',
  'remark': 'schema:Comment',
  'document-container': 'dossier:Serie',
  'document': 'dossier:Stuk'
};

let fullPathsCache = null;

const getFullPathsToAgenda = function() {
  if (fullPathsCache) {
    return fullPathsCache;
  }
  fullPathsCache = {};
  Object.keys(pathsToAgenda).map((type) => {
    buildFullPathsToAgendaForType(type);
  });
  return fullPathsCache;
};

const buildFullPathsToAgendaForType = function(type) {
  if (fullPathsCache[type]) {
    return fullPathsCache[type];
  }

  let paths = pathsToAgenda[type];
  if (!paths) {
    return [];
  }

  let result = paths.map((path) => {
    if (path.nextRDFType) {
      return buildFullPathsToAgendaForType(path.nextRDFType).map((next) => {
        return `${path.path} / ${next}`;
      });
    } else {
      return path;
    }
  });
  result = [].concat(...result);

  fullPathsCache[type] = result;
  return result;
};

const selectRelatedAgendasForSubjects = async function(subjects) {

  const pathsToAgenda = getFullPathsToAgenda();
  const restrictions = Object.keys(pathsToAgenda).map((typeName) => {
    return `
      ?subject a ${typeUris[typeName]} .
      ?subject (${pathsToAgenda[typeName].join(') | (')}) ?agenda .
    `;
  });

  restrictions.push(`
    ?subject a ${typeUris.agenda} .
    BIND(?subject AS ?agenda ).
  `);

  const agendas = new Set();

  for (let restriction of restrictions) {
    // the graph distinction here is meaningful. Only things in the original graph (kanselarij) should be examined,
    // otherwise the target can be in the public graph and can for instance be a type of document. in that case, almost all
    // documents will be examined

    const select = `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX dbpedia: <http://dbpedia.org/ontology/>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      PREFIX schema: <http://schema.org>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>

      SELECT DISTINCT ?agenda WHERE {
        GRAPH <http://mu.semte.ch/graphs/organizations/kanselarij> {
          VALUES (?subject) {
            (<${subjects.join('>) (<')}>)
          }
          ${restriction}
          ?agenda a ${typeUris.agenda} .
        }
      }`;

    const results = await directQuery(select);

    parseSparQlResults(JSON.parse(results))
      .map((item) => item.agenda)
      .forEach((agenda) => agendas.add(agenda));
  }

  return Array.from(agendas);
};

let subjectsToCheck = new Set();

const rememberDeltaSubjects = function(deltaset) {
  let subjects = subjectsToCheck;
  const addTripleUris = (triple) => {
    subjects.add(triple.subject.value);
    if (triple.object.type === 'uri') {
      subjects.add(triple.object.value);
    }
  };
  deltaset.inserts.map(addTripleUris);
  deltaset.deletes.map(addTripleUris);
  return subjects;
};


let checkingDeltas = false;
const checkAllDeltas = async function() {
  if (checkingDeltas) {
    return debouncedDelta();
  }
  checkingDeltas = true;
  let subjects = Array.from(subjectsToCheck);
  subjectsToCheck = new Set();

  await handleDeltaRelatedToAgenda(subjects);
  checkingDeltas = false;
};

const debouncedDelta = debounce(checkAllDeltas, DELTA_TIMEOUT);

const handleDelta = async function(req, res, newBuilders) {
  let body = req.body;
  builders = newBuilders;

  body.map((deltaset) => {
    rememberDeltaSubjects(deltaset);
  });
  debouncedDelta();
  res.send({ status: ok, statusCode: 200 });
};

module.exports = {
  handleDelta
};
