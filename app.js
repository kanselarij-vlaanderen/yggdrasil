import mu from 'mu';
import {query} from './repository/direct-sparql-endpoint';
import { parseSparQlResults } from './repository/helpers';
const app = mu.app;
const bodyParser = require('body-parser');
const cors = require('cors');
const cron = require('node-cron');
const DEBUG = process.env.DEBUG;

const fillInterneOverheid = require('./repository/fill-intern-overheid');
const fillInterneRegering = require('./repository/fill-intern-regering');
const fillPublic = require('./repository/fill-public');

app.use(bodyParser.json({ type: 'application/json' }));
app.use(cors());

const configurableQuery = function(queryString, direct){
  return query({
    sudo: true,
    url: direct?process.env.DIRECT_ENDPOINT:undefined
  }, queryString);
};

const directQuery = function(queryString){
  return configurableQuery(queryString, true);
};

const adminGraph = `http://mu.semte.ch/graphs/organizations/kanselarij`;

const queryEnvPublic = {
  tempGraph: `http://mu.semte.ch/temp/${mu.uuid()}`,
  adminGraph: adminGraph,
  targetGraph:`http://mu.semte.ch/graphs/public`,
  // the graph that traces lineage of uris to an agenda
  agendaLineageGraph: `http://mu.semte.ch/graphs/util/lineage`,
  run: configurableQuery
};

const queryEnvOverheid = {
  tempGraph: `http://mu.semte.ch/temp/${mu.uuid()}`,
  adminGraph: adminGraph,
  targetGraph:`http://mu.semte.ch/graphs/organizations/intern-overheid`,
  // the graph that traces lineage of uris to an agenda
  agendaLineageGraph: `http://mu.semte.ch/graphs/util/lineage`,
  run: configurableQuery
};

const queryEnvRegering = {
  tempGraph: `http://mu.semte.ch/temp/${mu.uuid()}`,
  adminGraph: adminGraph,
  targetGraph: `http://mu.semte.ch/graphs/organizations/intern-regering`,
  // the graph that traces lineage of uris to an agenda
  agendaLineageGraph: `http://mu.semte.ch/graphs/util/lineage`,
  run: configurableQuery
};


if(process.env.RELOAD_ALL_DATA_ON_INIT){

  const fillAll = async function(){
    let queryEnvOverheidSetup = Object.assign({}, queryEnvOverheid);
    let queryEnvRegeringSetup = Object.assign({}, queryEnvRegering);
    queryEnvOverheidSetup.run = directQuery;
    queryEnvRegeringSetup.run = directQuery;
    queryEnvPublic.run = directQuery;

    await fillPublic.fillUp(queryEnvPublic);
    //await fillInterneOverheid.fillUp(queryEnvOverheidSetup);
    //await fillInterneRegering.fillUp(queryEnvRegeringSetup);
  };
  fillAll();

}

const filterAgendaMustBeInSet = function(subjects, agendaVariable = "s"){
  return `FILTER( ?${agendaVariable} IN (<${subjects.join('>, <')}>))`;
};

const handleDeltaRelatedToAgenda = async function(subjects){
  if(DEBUG){
    console.log(`Found subjects: ${JSON.stringify(subjects)}`);
  }
  const relatedAgendas = selectRelatedAgendasForSubjects(subjects);
  if(DEBUG){
    console.log(`Related to subjects: ${relatedAgendas}`);
  }
  const filterAgendas = filterAgendaMustBeInSet(relatedAgendas);
  fillInterneOverheid.fillUp(queryEnvOverheid, filterAgendas);
  fillInterneRegering.fillUp(queryEnvRegering, filterAgendas);
};

const pathsToAgenda = {
  "agendaitem": ["dct:hasPart"],
  "procedurestap": ["besluitvorming:isGeagendeerdVia/dct:hasPart"],
  "meeting": ["^besluit:isAangemaaktVoor"],
  "access-level": [
    {path: "^ext:toegangsniveauVoorProcedurestap", nextRDFType: "subcase"},
    {path: "^ext:toegangsniveauVoorDocument", nextRDFType: "document"},
    {path: "^ext:toegangsniveauVoorDossier", nextRDFType: "case"}
  ],
  "newsletter-info":[
    {path: "^prov:generated", nextRDFType: "subcase"},
    {path: "^ext:algemeneNieuwsbrief", nextRDFType: "meeting"},
  ],
  "consultation-request": [
    {path: "^ext:bevatConsultatievraag", nextRDFType: "subcase"}
  ],
  "subcase-phase": [
    {path: "^ext:subcaseProcedurestapFase", nextRDFType: "subcase"},
    {path: "^ext:subcaseAgendapuntFase", nextRDFType: "agendaitem"}
  ],
  "decision": [
    {path: "^ext:procedurestapHeeftBesluit", nextRDFType: "decision"},
    {path: "^ext:agendapuntHeeftBesluit", nextRDFType: "agendaitem"}
  ],
  "meeting-record": [
    {path: "^ext:notulenVanAgendaPunt", nextRDFType: "agendaitem"},
    {path: "^ext:algemeneNotulen", nextRDFType: "meeting-record"}
  ],
  "case": [
    {path: "dct:hasPart", nextRDFType: "subcase"}
  ],
  "remark": [
    {path: "^ext:antwoorden*/^besluitvorming:opmerking", nextRDFType: "meeting"},
    {path: "^ext:antwoorden*/^besluitvorming:opmerking", nextRDFType: "newsletter-info"},
    {path: "^ext:antwoorden*/^besluitvorming:opmerking", nextRDFType: "document"},
    {path: "^ext:antwoorden*/^besluitvorming:opmerking", nextRDFType: "agendaitem"},
    {path: "^ext:antwoorden*/^besluitvorming:opmerking", nextRDFType: "decision"},
    {path: "^ext:antwoorden*/^besluitvorming:opmerking", nextRDFType: "case"},
    {path: "^ext:antwoorden*/^besluitvorming:opmerking", nextRDFType: "subcase"},
    {path: "^ext:antwoorden*/^besluitvorming:opmerking", nextRDFType: "decision"}
  ],
  "document": [
    {path: "^ext:beslissingFiche", nextRDFType: "document" },
    {path: "besluitvorming:heeftVersie", nextRDFType: "document-version"},
    {path: "^ext:getekendeNotulen", nextRDFType: "meeting-record"}
  ],
  "announcement": [
    ["ext:mededeling"]
  ],
  "documentversion": [
    {path: "^ext:bevatDocumentversie", nextRDFType: "subcase"},
    {path: "^ext:documentenVoorPublicatie", nextRDFType: "newsletter-info" },
    {path: "^ext:documentenVoorPublicatie", nextRDFType: "newsletter-info" },
    {path: "^ext:mededelingBevatDocumentversie", nextRDFType: "announcement" },
    {path: "^ext:documentenVoorBeslissing", nextRDFType: "decision"},
    {path: "^ext:getekendeDocumentVersiesVoorNotulen", nextRDFType: "meeting-record"}
  ]
};

const typeUris = {
  "agenda": "besluitvorming:Agenda",
  "agendaitem": "besluit:Agendapunt",
  "subcase": "dbpedia:UnitOfWork",
  "meeting": "besluit:Zitting",
  "access-level": "ext:ToegangsniveauCode",
  "newsletter-info": "besluitvorming:NieuwsbriefInfo",
  "consultation-request": "besluitvorming:Consultatievraag" ,
  "subcase-phase": "ext:ProcedurestapFase",
  "decision": "besluit:Besluit",
  "meeting-record": "ext:Notule",
  "case": "dbpedia:Case",
  "remark": "schema:Comment",
  "document": "foaf:Document",
  "announcement": "besluitvorming:Mededeling",
  "document-version": "ext:DocumentVersie"
};

let fullPathsCache = null;

const getFullPathsToAgenda = function(){
  if(fullPathsCache){
    return fullPathsCache;
  }
  fullPathsCache = {};
  Object.keys(pathsToAgenda).map((type) => {
    buildFullPathsToAgendaForType(type);
  });
  return fullPathsCache;
};

const buildFullPathsToAgendaForType = function(type){
  if(fullPathsCache[type]){
    return fullPathsCache[type];
  }

  let paths = pathsToAgenda[type];
  if(!paths){
    return [];
  }

  const result = paths.map((path) => {
    if(path.nextRDFType){
      return getFullPathsToAgendaForType(path.nextRDFType).map((next) => {
        return `${path.path} / ${next}`;
      });
    }else{
      return path.path;
    }
  }).flat();

  fullPathsCache[type] = result;
  return result;
};

const selectRelatedAgendasForSubjects = async function(subjects){
  const pathsToAgenda = getFullPathsToAgenda();
  const unions = Object.keys(pathsToAgenda).map((typeName) => {
    return `{ 
      ?subject a ${typeUris[typeName]} .
      ?subject (${pathsToAgenda[typeName].join(" | ")}) ?agenda .
    }`
  }).join(' UNION ');

  const select = `SELECT DISTINCT ?agenda WHERE {
    VALUES (?subject) {
      <${[...subjects].join('> <')}>
    }
    { {
      ?subject a ${typeUris.agenda} .
      BIND(?subject) AS ?agenda.
    } UNION ${unions} }
  }`;

  const results = await query(select);
  return parseSparQlResults(results);
};

const grabDeltaSubjects = function(deltaset){
  let subjects = new Set();
  const addTripleUris = (triple) => {
    subjects.add(triple.subject);
    if(triple.object.type == "uri"){
      subjects.add(triple.object);
    }
  };
  deltaset.inserts.map(addTripleUris);
  deltaset.deletes.map(addTripleUris);
  return subjects;
};

const handleDelta = async function(req,res){
  let body = req.body;

  body.map((deltaset) => {
    const insertSubjects = grabDeltaSubjects(deltaset);
    handleDeltaRelatedToAgenda(insertSubjects);
  });
  res.send({ status: ok, statusCode: 200});
};

app.post('/delta', (req, res) => {
  return handleDelta(req, res);
});

