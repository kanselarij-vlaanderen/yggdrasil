import mu from 'mu';
import {query} from './repository/direct-sparql-endpoint';
import { parseSparQlResults } from './repository/helpers';
const app = mu.app;
const bodyParser = require('body-parser');
const cors = require('cors');
const cron = require('node-cron');
const DEBUG = process.env.DEBUG == "true";
import { ok } from 'assert';
import debounce from 'debounce';


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
  adminGraph: adminGraph,
  targetGraph:`http://mu.semte.ch/graphs/public`,
  fullRebuild: false,
  run: configurableQuery
};

const queryEnvOverheid = {
  adminGraph: adminGraph,
  targetGraph:`http://mu.semte.ch/graphs/organizations/intern-overheid`,
  fullRebuild: false,
  run: configurableQuery
};

const queryEnvRegering = {
  adminGraph: adminGraph,
  targetGraph: `http://mu.semte.ch/graphs/organizations/intern-regering`,
  fullRebuild: false,
  run: configurableQuery
};


if(process.env.RELOAD_ALL_DATA_ON_INIT == "true"){

  const fillAll = async function(){
    let queryEnvOverheidSetup = Object.assign({}, queryEnvOverheid);
    let queryEnvRegeringSetup = Object.assign({}, queryEnvRegering);
    queryEnvOverheidSetup.fullRebuild = true;
    queryEnvRegeringSetup.fullRebuild = true;
    queryEnvOverheidSetup.run = directQuery;
    queryEnvRegeringSetup.run = directQuery;
    queryEnvPublic.run = directQuery;

    await fillPublic.fillUp(queryEnvPublic);
    await fillInterneOverheid.fillUp(queryEnvOverheidSetup);
    await fillInterneRegering.fillUp(queryEnvRegeringSetup);
  };
  fillAll();

}

const handleDeltaRelatedToAgenda = async function(subjects, queryEnv){
  if(DEBUG){
    console.log(`Found subjects: ${JSON.stringify(subjects)}`);
  }
  const relatedAgendas = await selectRelatedAgendasForSubjects(subjects);
  if(DEBUG){
    console.log(`Related to subjects: ${relatedAgendas}`);
  }
  if(!relatedAgendas || !relatedAgendas.length){
    return;
  }
  await fillInterneOverheid.fillUp(queryEnvOverheid, relatedAgendas);
  await fillInterneRegering.fillUp(queryEnvRegering, relatedAgendas);
};

const pathsToAgenda = {
  "agendaitem": ["^dct:hasPart"],
  "subcase": ["besluitvorming:isGeagendeerdVia / ^dct:hasPart"],
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
    {path: "^ext:procedurestapHeeftBesluit", nextRDFType: "subcase"},
    {path: "^ext:agendapuntHeeftBesluit", nextRDFType: "agendaitem"}
  ],
  "meeting-record": [
    {path: "^ext:notulenVanAgendaPunt", nextRDFType: "agendaitem"},
    {path: "^ext:algemeneNotulen", nextRDFType: "meeting"}
  ],
  "case": [
    {path: "dct:hasPart", nextRDFType: "subcase"}
  ],
  "remark": [
    {path: "^ext:antwoorden* / ^besluitvorming:opmerking", nextRDFType: "meeting"},
    {path: "^ext:antwoorden* / ^besluitvorming:opmerking", nextRDFType: "newsletter-info"},
    {path: "^ext:antwoorden* / ^besluitvorming:opmerking", nextRDFType: "document"},
    {path: "^ext:antwoorden* / ^besluitvorming:opmerking", nextRDFType: "agendaitem"},
    {path: "^ext:antwoorden* / ^besluitvorming:opmerking", nextRDFType: "decision"},
    {path: "^ext:antwoorden* / ^besluitvorming:opmerking", nextRDFType: "case"},
    {path: "^ext:antwoorden* / ^besluitvorming:opmerking", nextRDFType: "subcase"},
    {path: "^ext:antwoorden* / ^besluitvorming:opmerking", nextRDFType: "decision"}
  ],
  "document": [
    {path: "^ext:beslissingFiche", nextRDFType: "decision" },
    {path: "besluitvorming:heeftVersie", nextRDFType: "document-version"},
    {path: "^ext:getekendeNotulen", nextRDFType: "meeting-record"}
  ],
  "announcement": [
    "ext:mededeling"
  ],
  "document-version": [
    {path: "^ext:bevatDocumentversie", nextRDFType: "subcase"},
    {path: "^ext:bevatAgendapuntDocumentversie", nextRDFType: "agendaitem"},
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

  let result = paths.map((path) => {
    if(path.nextRDFType){
      return buildFullPathsToAgendaForType(path.nextRDFType).map((next) => {
        return `${path.path} / ${next}`;
      });
    }else{
      return path;
    }
  });
  result = [].concat(...result);

  fullPathsCache[type] = result;
  return result;
};

const selectRelatedAgendasForSubjects = async function(subjects){

  const pathsToAgenda = getFullPathsToAgenda();
  const restrictions = Object.keys(pathsToAgenda).map((typeName) => {
    return ` 
      ?subject a ${typeUris[typeName]} .
      ?subject (${pathsToAgenda[typeName].join(") | (")}) ?agenda .
    `
  });

  restrictions.push(`
    ?subject a ${typeUris.agenda} .
    BIND(?subject AS ?agenda ).
  `);

  const agendas = new Set();
  return Promise.all(restrictions.map(async (restriction) => {
    const select = `
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>
  PREFIX schema: <http://schema.org>
  
  SELECT DISTINCT ?agenda WHERE {
    VALUES (?subject) {
      (<${subjects.join('>) (<')}>)
    }
    
    ${restriction}
    
    ?agenda a ${typeUris.agenda} .
  }`;
    const results = await directQuery(select);
    parseSparQlResults(JSON.parse(results)).map((item) => {
      return item.agenda;
    }).map((agenda) => {
      agendas.add(agenda);
    });
  })).then(() => {
    return Array.from(agendas);
  });
};

let subjectsToCheck = new Set();

const rememberDeltaSubjects = function(deltaset){
  let subjects = subjectsToCheck;
  const addTripleUris = (triple) => {
    subjects.add(triple.subject.value);
    if(triple.object.type == "uri"){
      subjects.add(triple.object.value);
    }
  };
  deltaset.inserts.map(addTripleUris);
  deltaset.deletes.map(addTripleUris);
  return subjects;
};


let checkingDeltas = false;
const checkAllDeltas = async function(){
  if(checkingDeltas){
    return debouncedDelta();
  }
  checkingDeltas = true;
  let subjects = Array.from(subjectsToCheck);
  subjectsToCheck = new Set();

  await handleDeltaRelatedToAgenda(subjects);
  checkingDeltas = false;
};

const debouncedDelta = debounce(checkAllDeltas, 10000);

const handleDelta = async function(req,res){
  let body = req.body;

  body.map((deltaset) => {
    rememberDeltaSubjects(deltaset);
  });
  debouncedDelta();
  res.send({ status: ok, statusCode: 200});
};

app.post('/delta', (req, res) => {
  return handleDelta(req, res);
});

