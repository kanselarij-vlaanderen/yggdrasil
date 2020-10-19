import mu from 'mu';
import moment from 'moment';
import { query } from './direct-sparql-endpoint';

const batchSize = process.env.BATCH_SIZE || 3000;
const smallBatchSize = process.env.SMALL_BATCH_SIZE || 100;
const minimalBatchSize = process.env.MINIMAL_BATCH_SIZE || 100;

const parseSparQlResults = (data, multiValueProperties = []) => {
  const vars = data.head.vars;
  return data.results.bindings.map(binding => {
    let obj = {};

    vars.forEach(varKey => {
      if (binding[varKey]) {
        let val = binding[varKey].value;
        if (multiValueProperties.includes(varKey)) {
          val = val.split('|');
        }
        obj[varKey] = val;
      } else {
        obj[varKey] = null;
      }
    });
    return obj;
  });
};

const logStage = (start, logMessage, graph) => {
  const time = moment().utc().diff(start, 'seconds', true);
  console.log(`${graph} => ${logMessage} -- time: ${time.toFixed(3)}s`);
};

const removeInfoNotInTemp = (queryEnv) => {
  // TODO should we not batch this delete?
  const query = `
  DELETE {
    GRAPH <${queryEnv.targetGraph}> {
      ?s ?p ?o.
    }
  } WHERE {
    GRAPH <${queryEnv.targetGraph}> {
      ?s ?p ?o.
      ?s a ?type.

      FILTER NOT EXISTS {
        GRAPH <${queryEnv.tempGraph}> {
          ?s ?p ?o.
        }
      }
    }
  }`;
  return queryEnv.run(query);
};

// TODO better do the inverse, but that means we should do it on items that truly have confidentiality... to be seen
const notConfidentialFilter = `
    FILTER NOT EXISTS {
      ?s ext:vertrouwelijk "true"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> .
    }
`;

const notInternRegeringFilter = `
    FILTER NOT EXISTS {
      ?s ?accessPredicate <http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/d335f7e3-aefd-4f93-81a2-1629c2edafa3> .
      VALUES (?accessPredicate ) {
        ( <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorProcedurestap> )
        ( <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDocumentVersie> )
        ( <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDossier> )
      }
    }
`;

const notInternOverheidFilter = `
    FILTER NOT EXISTS {
      ?s ?accessPredicate ?levelTooTough .
      VALUES (?levelTooTough) {
        ( <http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/abe4c18d-13a9-45f0-8cdd-c493eabbbe29> )
        ( <http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/d335f7e3-aefd-4f93-81a2-1629c2edafa3> ) .
      }
      VALUES (?accessPredicate ) {
        ( <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorProcedurestap> )
        ( <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDocumentVersie> )
        ( <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDossier> )
      }
    }
`;

const transformFilter = (originalFilter, newTargetVariable, pathToTarget) => {
  const newFilter = originalFilter.split('?s ').join(`${newTargetVariable} `);
  return newFilter.split('NOT EXISTS {').join(`NOT EXISTS {
    ${pathToTarget}`);
};

const addRelatedFiles = (queryEnv, extraFilters) => {
  extraFilters = extraFilters || '';

  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a nfo:FileDataObject .
      ?second a nfo:FileDataObject .
      ?s ext:tracesLineageTo ?agenda .
      ?second ext:tracesLineageTo ?agenda .
    }
  } WHERE {
    GRAPH <${queryEnv.tempGraph}> {
      ?target a ?targetClass .
      ?target ext:tracesLineageTo ?agenda .
    }
    GRAPH <${queryEnv.adminGraph}> {
      ?s a nfo:FileDataObject .
      ?target ext:file ?s.

      OPTIONAL {
        ?second <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource> ?s.
      }

      ${extraFilters}
    }
  }`;
  return queryEnv.run(query, true);
};

async function cleanup() {
  const result = JSON.parse(await directQuery("PREFIX ext: <http://mu.semte.ch/vocabularies/ext/> SELECT ?g WHERE { GRAPH ?g { ?g a ext:TempGraph }}"));
  if (result.results && result.results.bindings) {
    console.log(`found ${result.results.bindings.length} old temporary graphs, removing before going further`);
    for (let binding of result.results.bindings) {
      console.log(`dropping graph ${binding.g.value}`);
      await directQuery(`DROP SILENT GRAPH <${binding.g.value}>`);
    }
  }
};

const fillOutDetailsOnVisibleItemsLeft = async (queryEnv) => {
  const result = await queryEnv.run(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    SELECT DISTINCT ?s WHERE {
      GRAPH <${queryEnv.tempGraph}> {
        ?s ext:tracesLineageTo ?agenda .
      }
      FILTER NOT EXISTS {
        GRAPH <${queryEnv.tempGraph}> {
          ?s ext:yggdrasilLeft ?s.
        }
      }
    } LIMIT ${batchSize}`, true);

  const targets = JSON.parse(result).results.bindings.map((binding) => binding.s.value);

  for (let target of targets) {
    const query = `
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      INSERT {
        GRAPH <${queryEnv.tempGraph}> {
          <${target}> ?p ?o.
        }
      } WHERE {
        VALUES ( ?s ) {
           ( <${target}> )
        }
        GRAPH <${queryEnv.tempGraph}> {
                   ?s a ?thing .
                   ?s ext:tracesLineageTo ?agenda .
        }
        GRAPH <${queryEnv.adminGraph}> {
            ?s ?p ?o.
        }
      }`;
    await queryEnv.run(query, true);

    // mark done as separate step because transactional behaviour of queries might not actually be trustworthy
    await queryEnv.run(`
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      INSERT DATA {
        GRAPH <${queryEnv.tempGraph}> {
           <${target}> ext:yggdrasilLeft <${target}> .
        }
      }`, true);
  }
};

const fillOutDetailsOnVisibleItemsRight = async (queryEnv) => {
  const result = await queryEnv.run(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    SELECT DISTINCT ?s WHERE {
      GRAPH <${queryEnv.tempGraph}> {
        ?s ext:tracesLineageTo ?agenda .
      }
      FILTER NOT EXISTS {
        GRAPH <${queryEnv.tempGraph}> {
          ?s ext:yggdrasilRight ?s.
        }
      }
    } LIMIT ${batchSize}`, true);

  const targets = JSON.parse(result).results.bindings.map((binding) => binding.s.value);

  for (let target of targets) {
    const query = `
      INSERT {
        GRAPH <${queryEnv.tempGraph}> {
          ?oo ?pp ?s.
        }
      } WHERE {
        VALUES ( ?s ) {
           ( <${target}> )
        }
        GRAPH <${queryEnv.tempGraph}> {
           ?s a ?thing .
        }
        GRAPH <${queryEnv.adminGraph}> {
           ?oo ?pp ?s.
        }
    }`;
    await queryEnv.run(query, true);

    // mark done as separate step because transactional behaviour of queries might not actually be trustworthy
    await queryEnv.run(`
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      INSERT DATA {
        GRAPH <${queryEnv.tempGraph}> {
           <${target}> ext:yggdrasilRight <${target}> .
        }
      }`, true);
  }
};

const repeatUntilTripleCountConstant = async function(fun, queryEnv, previousCount, graph) {
  const startQ = moment();
  const funResult = await fun();
  const timeQuery = moment().diff(startQ, 'seconds', true).toFixed(3);
  graph = graph || queryEnv.tempGraph;
  const start = moment();
  const query = `SELECT (COUNT(?s) AS ?count) WHERE {
    GRAPH <${graph}> {
      ?s ?p ?o.
    }
  }`;
  return queryEnv.run(query, true).then((result) => {
    const timeCount = moment().diff(start, 'seconds', true).toFixed(3);
    let count = 0;
    try {
      count = Number.parseInt(JSON.parse(result).results.bindings[0].count.value);
      console.log(`<${graph}> size is now ${count}... -- q: ${timeQuery}s, t: ${timeCount}s`);
    } catch (e) {
      console.log('no matching results');
    }
    if (count == previousCount){
      return funResult;
    } else {
      return repeatUntilTripleCountConstant(fun, queryEnv, count, graph);
    }
  });
};

const fillOutDetailsOnVisibleItems = async (queryEnv) => {
  await repeatUntilTripleCountConstant(() => {
    return fillOutDetailsOnVisibleItemsLeft(queryEnv);
  }, queryEnv, 0);
  await repeatUntilTripleCountConstant(() => {
      return fillOutDetailsOnVisibleItemsRight(queryEnv);
  }, queryEnv, 0);
};

const addAllRelatedDocuments = async (queryEnv, extraFilters) => {
  extraFilters = extraFilters || '';

  const queryTemplate = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>

  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a dossier:Stuk .
      ?s ext:tracesLineageTo ?agenda .
      ?container a dossier:Serie .
      ?container ext:tracesLineageTo ?agenda .
    }
  } WHERE {
    { SELECT ?target ?agenda WHERE {
      GRAPH <${queryEnv.tempGraph}> {
        ?target a ?targetClass .
        ?target ext:tracesLineageTo ?agenda .
      }
    } }
    GRAPH <${queryEnv.adminGraph}> {
      $REPLACECONSTRAINT
      FILTER NOT EXISTS {
        GRAPH <${queryEnv.tempGraph}> {
          ?s a dossier:Stuk .
        }
      }
      OPTIONAL {
        ?container dossier:collectie.bestaatUit ?s .
      }

      ${extraFilters}

    }
  }`;
  // TODO: KAS-1420: ext:documentenVoorBeslissing zou eventueel na bevestiging weg mogen. te bekijken.
  const constraints = [
    `
      ?target ( ext:bevatDocumentversie | ext:zittingDocumentversie | ext:bevatReedsBezorgdeDocumentversie | besluitvorming:geagendeerdStuk | ext:bevatReedsBezorgdAgendapuntDocumentversie | ext:documentenVoorPublicatie | ext:documentenVoorBeslissing | dct:hasPart | prov:generated | besluitvorming:genereertVerslag ) ?s .
      ?s a dossier:Stuk .
    `,
    `
      ?target ( dct:hasPart ) / dossier:collectie.bestaatUit ?s .
      ?s a dossier:Stuk .
    `
  ];

  await queryEnv.run(queryTemplate.split('$REPLACECONSTRAINT').join(constraints[0]), true);
  await queryEnv.run(queryTemplate.split('$REPLACECONSTRAINT').join(constraints[1]), true);
};

const addAllVisibleRelatedDocuments = async (queryEnv, extraFilters = "") => {
  return addAllRelatedDocuments(queryEnv, `
      ?s <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDocumentVersie> ?anyAccessLevel .

      ${extraFilters}
`);
};

const addAllRelatedToAgenda = (queryEnv, extraFilters, relationProperties) => {
  relationProperties = relationProperties || ['dct:hasPart', 'besluitvorming:isAgendaVoor', '^besluitvorming:behandelt','(dct:hasPart / ^besluitvorming:genereertAgendapunt)', '( dct:hasPart / ^besluitvorming:genereertAgendapunt / besluitvorming:vindtPlaatsTijdens )'];
  extraFilters = extraFilters || '';

  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a ?thing .
      ?s ext:tracesLineageTo ?agenda .
    }
  } WHERE {
    GRAPH <${queryEnv.tempGraph}> {
      ?agenda a besluitvorming:Agenda .
    }
    GRAPH <${queryEnv.adminGraph}> {
      ?agenda ( ${relationProperties.join(' | ')} ) ?s .
      ?s a ?thing .

      { { ?s a dossier:Dossier .
          ?s dossier:doorloopt / ^besluitvorming:vindtPlaatsTijdens / besluitvorming:genereertAgendapunt ?agendaitem .
          ?agenda dct:hasPart ?agendaitem .
          ?agendaitem ext:formeelOK <http://kanselarij.vo.data.gift/id/concept/goedkeurings-statussen/CC12A7DB-A73A-4589-9D53-F3C2F4A40636>.
        }
        UNION
        { ?s ext:formeelOK <http://kanselarij.vo.data.gift/id/concept/goedkeurings-statussen/CC12A7DB-A73A-4589-9D53-F3C2F4A40636> . }
        UNION
        { FILTER NOT EXISTS {
            VALUES (?restrictedType) {
              (dossier:Dossier) (besluit:AgendaPunt)
            }
            ?s a ?restrictedType.
          } }}

      ${extraFilters}
    }
  }`;
  return queryEnv.run(query, true);
};

const addRelatedToAgendaItemBatched = async (queryEnv, extraFilters) => {
  extraFilters = extraFilters || '';

  const query = `
   PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
   PREFIX dct: <http://purl.org/dc/terms/>
   PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
   PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
   PREFIX dbpedia: <http://dbpedia.org/ontology/>
   PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
   PREFIX prov: <http://www.w3.org/ns/prov#>
   PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
   
   SELECT ?s ?thing ?agenda WHERE {
     { SELECT ?target ?agenda WHERE {
       GRAPH <${queryEnv.tempGraph}> {
         ?target a besluit:Agendapunt .
         ?target ext:tracesLineageTo ?agenda .
       }
     }}
     GRAPH <${queryEnv.adminGraph}> {
       ?target ( ext:bevatReedsBezorgdAgendapuntDocumentversie | ext:agendapuntGoedkeuring | ^besluitvorming:genereertAgendapunt | ^besluitvorming:genereertAgendapunt / besluitvorming:vindtPlaatsTijdens | ^besluitvorming:heeftOnderwerp) ?s .
       ?s a ?thing .

       FILTER NOT EXISTS {
         GRAPH <${queryEnv.tempGraph}> {
           ?s ext:tracesLineageTo ?agenda .
         }
       }

      ${extraFilters}
    }
  } LIMIT ${smallBatchSize}`;

  const result = await queryEnv.run(query, true);

  const targets = JSON.parse(result).results.bindings.map((binding) => {
    return `<${binding.s.value}> ext:tracesLineageTo <${binding.agenda.value}> .
<${binding.s.value}> a <${binding.thing.value}> .`;
  });

  if (targets.length) {
    const update = `
   PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

   INSERT DATA {
     GRAPH <${queryEnv.tempGraph}> {
       ${targets.join('\n')}
     }
   }`;
    return await queryEnv.run(update, true);
  }
};

const addRelatedToAgendaItem = async (queryEnv, extraFilters) => {
  await repeatUntilTripleCountConstant(() => {
    return addRelatedToAgendaItemBatched(queryEnv, extraFilters);
  }, queryEnv, 0, queryEnv.tempGraph);
};

const addRelatedToSubcaseBatched = async (queryEnv, extraFilters) => {
  extraFilters = extraFilters || '';

  let query = `
   PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
   PREFIX dct: <http://purl.org/dc/terms/>
   PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
   PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
   PREFIX dbpedia: <http://dbpedia.org/ontology/>
   PREFIX prov: <http://www.w3.org/ns/prov#>
   PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
   PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
   PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
   
   SELECT ?s ?thing ?agenda WHERE {
                 { SELECT ?target ?agenda WHERE {
                   GRAPH <${queryEnv.tempGraph}> {
                           ?target a dossier:Procedurestap .
                           ?target ext:tracesLineageTo ?agenda .
                   }
                 }}

     GRAPH <${queryEnv.adminGraph}> {
       ?target ( ext:bevatReedsBezorgdeDocumentversie | ^dossier:doorloopt | ext:bevatConsultatievraag | ext:procedurestapGoedkeuring ) ?s .
       ?s a ?thing .

       FILTER NOT EXISTS {
         GRAPH <${queryEnv.tempGraph}> {
           ?s ext:tracesLineageTo ?agenda .
                                 }
       }

       ${extraFilters}
    }
  } LIMIT ${smallBatchSize}`;

  const result = await queryEnv.run(query, true);

  const targets = JSON.parse(result).results.bindings.map((binding) => {
    return `<${binding.s.value}> ext:tracesLineageTo <${binding.agenda.value}> .
<${binding.s.value}> a <${binding.thing.value}> .`;
  });

  if (targets.length) {
    const update = `
   PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

   INSERT DATA {
     GRAPH <${queryEnv.tempGraph}> {
       ${targets.join('\n')}
     }
   }`;
    return await queryEnv.run(update, true);
  }
};

const addRelatedToSubcase = async (queryEnv, extraFilters) => {
  await repeatUntilTripleCountConstant(() => {
    return addRelatedToSubcaseBatched(queryEnv, extraFilters);
  }, queryEnv, 0, queryEnv.tempGraph);
};

const addRelatedToAgendaItemAndSubcase = async (queryEnv, extraFilters) => {
  await addRelatedToAgendaItem(queryEnv, extraFilters);
  await addRelatedToSubcase(queryEnv, extraFilters);
};

const runStage = async function(message, queryEnv, stage) {
  const stageStart = moment().utc();
  await stage();
  logStage(stageStart, message, queryEnv.targetGraph);
};

const removeThingsWithLineageNoLongerInTempBatched = async function(queryEnv, targetedAgendas) {
  if (!targetedAgendas) {
    return;
  }

  const result = await queryEnv.run(`PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT DISTINCT ?s WHERE {
      VALUES (?agenda) {
        (<${targetedAgendas.join('>) (<')}>)
      }
      GRAPH <${queryEnv.targetGraph}> {
        ?s ext:tracesLineageTo ?agenda .
      }
      FILTER NOT EXISTS {
        GRAPH <${queryEnv.tempGraph}> {
          ?s ext:tracesLineageTo ?anyTargetedAgenda.
        }
      }
    } LIMIT ${smallBatchSize}`, true);

  const targets = JSON.parse(result).results.bindings.map((binding) => binding.s.value);

  for (let target of targets) {
    const queryRight = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    DELETE {
      GRAPH <${queryEnv.targetGraph}> {
        ?s ?p ?o .
      }
    } WHERE {
      VALUES ( ?s ) {
        ( <${target}> )
      }
      GRAPH <${queryEnv.targetGraph}> {
        ?s ?p ?o .
      }
    }`;
    await queryEnv.run(queryRight);

    const queryLeft = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    DELETE {
      GRAPH <${queryEnv.targetGraph}> {
        ?oo ?pp ?s .
      }
    } WHERE {
      VALUES ( ?s ) {
        ( <${target}> )
      }
      GRAPH <${queryEnv.targetGraph}> {
        ?oo ?pp ?s .
      }
    }`;
    await queryEnv.run(queryLeft);
  }
};

const removeLineageWhereLineageNoLongerInTempBatched = async function(queryEnv, targetedAgendas) {
  if (!targetedAgendas) {
    return;
  }

  const result = await queryEnv.run(`
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  SELECT ?s ?agenda WHERE {
    VALUES (?agenda) {
      (<${targetedAgendas.join('>) (<')}>)
    }
    GRAPH <${queryEnv.targetGraph}> {
      ?s ext:tracesLineageTo ?agenda .
    }
    FILTER NOT EXISTS {
      GRAPH <${queryEnv.tempGraph}> {
        ?s ext:tracesLineageTo ?agenda .
      }
    }
  } LIMIT ${minimalBatchSize}
  `, true);

  const targets = JSON.parse(result).results.bindings.map((binding) => {
    return `<${binding.s.value}> ext:tracesLineageTo <${binding.agenda.value}> .`;
  });

  if (targets.length) {
    const query = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    DELETE DATA {
      GRAPH <${queryEnv.targetGraph}> {
        ${targets.join('\n')}
      }
    }`;
    await queryEnv.run(query);
  }
};

const removeThingsWithLineageNoLongerInTemp = async function(queryEnv, targetedAgendas) {
  await repeatUntilTripleCountConstant(() => {
    return removeThingsWithLineageNoLongerInTempBatched(queryEnv, targetedAgendas);
  }, queryEnv, 0, queryEnv.targetGraph);
  await repeatUntilTripleCountConstant(() => {
    return removeLineageWhereLineageNoLongerInTempBatched(queryEnv, targetedAgendas);
  }, queryEnv, 0, queryEnv.targetGraph);
};

const logTempGraphSummary = async (queryEnv) => {
  console.log('== Temp graph summary ==');
  const result = await queryEnv.run(`
    SELECT (COUNT(?s) AS ?count) ?thing WHERE {
      GRAPH <${queryEnv.tempGraph}> {
        ?s a ?thing
      }
    } GROUP BY ?thing
  `, true);

  const bindings = JSON.parse(result).results.bindings;
  for (let binding of bindings) {
    console.log(`[${binding['count'].value}] ${binding['thing'].value}`);
  }
};

const copyTempToTarget = async function(queryEnv) {
  await logTempGraphSummary(queryEnv);
  return repeatUntilTripleCountConstant(() => {
    return copySetOfTempToTarget(queryEnv);
  }, queryEnv, 0, queryEnv.tempGraph);
};

const copySetOfTempToTarget = async function(queryEnv) {
  const result = await queryEnv.run(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    SELECT DISTINCT ?s WHERE {
      GRAPH <${queryEnv.tempGraph}> {
        ?s a ?thing .
      }
      FILTER NOT EXISTS {
        GRAPH <${queryEnv.tempGraph}> {
          ?s ext:yggdrasilMoved ?s .
        }
      }
    } LIMIT ${smallBatchSize}`, true);
  const targets = JSON.parse(result).results.bindings.map((binding) => binding.s.value);

  for (let target of targets) {
    const query = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    INSERT {
      GRAPH <${queryEnv.targetGraph}> {
        <${target}> ?p ?o .
      }
    } WHERE {
      GRAPH <${queryEnv.tempGraph}> {
        <${target}> ?p ?o .
        FILTER (?p NOT IN ( ext:yggdrasilLeft, ext:yggdrasilRight ) )
      }
    }`;
    await queryEnv.run(query);

    // mark done as separate step because transactional behaviour of queries might not actually be trustworthy
    await queryEnv.run(`
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      INSERT DATA {
        GRAPH <${queryEnv.tempGraph}> {
          <${target}> ext:yggdrasilMoved <${target}>
        }
      }`, true);
  }
};

const removeStalePropertiesOfLineageBatch = async function(queryEnv, targetedAgendas) {
  if (!targetedAgendas) {
    return;
  }
  const result = await queryEnv.run(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT DISTINCT ?s WHERE {
      VALUES (?agenda) {
        (<${targetedAgendas.join('>) (<')}>)
      }
      GRAPH <${queryEnv.targetGraph}> {
        ?s ext:tracesLineageTo ?agenda .
      }
      { {
            GRAPH <${queryEnv.targetGraph}> {
              ?s ?p ?o .
            }
            FILTER NOT EXISTS {
              GRAPH <${queryEnv.tempGraph}> {
                ?s ?p ?o.
              }
            }
        } UNION {
            GRAPH <${queryEnv.targetGraph}> {
              ?oo ?pp ?s .
            }
            FILTER NOT EXISTS {
              GRAPH <${queryEnv.tempGraph}> {
                ?oo ?pp ?s.
              }
            }
      } }
    } LIMIT ${batchSize}`, true);
  const targets = JSON.parse(result).results.bindings.map((binding) => {
    return binding.s.value;
  });
  if (targets.length === 0) {
    return;
  }

  const query = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    DELETE {
      GRAPH <${queryEnv.targetGraph}> {
        ?s ?p ?o .
        ?oo ?pp ?s .
      }
    } WHERE {
      VALUES (?s) {
        ( <${targets.join('>) (<')}> )
      }
      { {
           GRAPH <${queryEnv.targetGraph}> {
             ?s ?p ?o .
           }
           FILTER NOT EXISTS {
             GRAPH <${queryEnv.tempGraph}> {
               ?s ?p ?o.
             }
           }
         } UNION {
            GRAPH <${queryEnv.targetGraph}> {
              ?oo ?pp ?s .
            }
            FILTER NOT EXISTS {
              GRAPH <${queryEnv.tempGraph}> {
                ?oo ?pp ?s.
              }
            }
      } }
    }`;
  await queryEnv.run(query);
};

const removeStalePropertiesOfLineage = async function(queryEnv, targetedAgendas) {
  return repeatUntilTripleCountConstant(() => {
    return removeStalePropertiesOfLineageBatch(queryEnv, targetedAgendas);
  }, queryEnv, 0, queryEnv.targetGraph);
};

const cleanupBasedOnLineage = async function(queryEnv, targetedAgendas) {
  await removeThingsWithLineageNoLongerInTemp(queryEnv, targetedAgendas);
  await removeStalePropertiesOfLineage(queryEnv, targetedAgendas);
};

const filterAgendaMustBeInSet = function(subjects, agendaVariable = 's') {
  if (!subjects || !subjects.length) {
    return '';
  }
  return `VALUES (?${agendaVariable}) {(<${subjects.join('>) (<')}>)}`;
};

const addAllTreatments = (queryEnv, extraFilters) => {
  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX brc: <http://kanselarij.vo.data.gift/id/concept/beslissings-resultaat-codes/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a besluit:BehandelingVanAgendapunt.
      ?s ext:tracesLineageTo ?agenda.
    }
  } WHERE {
    GRAPH <${queryEnv.tempGraph}> {
      ?agendaitem a besluit:Agendapunt.
      ?agendaitem ext:tracesLineageTo ?agenda.
    }
    GRAPH <${queryEnv.adminGraph}> {
      ?agenda dct:hasPart ?agendaitem.
      ?agenda besluitvorming:isAgendaVoor ?session.
      ?session ext:releasedDecisions ?date.
      ?s besluitvorming:heeftOnderwerp ?agendaitem.
      ${extraFilters}
    }
  }`;
  return queryEnv.run(query, true);
};

const addVisibleDecisions = (queryEnv, extraFilters) => {
  return addAllTreatments(queryEnv, `
    ?agenda besluitvorming:isAgendaVoor ?session.
    ?session ext:releasedDecisions ?date.
    ?s besluitvorming:resultaat brc:56312c4b-9d2a-4735-b0b1-2ff14bb524fd .
 
    ${extraFilters}
`);
};

const generateTempGraph = async function(queryEnv) {
  const tempGraph = `http://mu.semte.ch/temp/${mu.uuid()}`;
  queryEnv.tempGraph = tempGraph;
  await queryEnv.run(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    INSERT DATA {
      GRAPH <${tempGraph}> {
        <${tempGraph}> a ext:TempGraph .
      }
    }`, true);
};

const addAllNewsletterInfo = async (queryEnv, extraFilters) => {
  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a besluitvorming:NieuwsbriefInfo .
      ?s ext:tracesLineageTo ?agenda.
    }
  } WHERE {
    GRAPH <${queryEnv.tempGraph}> {
      ?target a ?thing .
      ?target ext:tracesLineageTo ?agenda.
    }
    GRAPH <${queryEnv.adminGraph}> {
      ?target (prov:generated | ext:algemeneNieuwsbrief ) ?s .

      ${extraFilters}
    }
  }`;
  return queryEnv.run(query, true);
};


const addVisibleNewsletterInfo = async (queryEnv, extraFilters) => {
  return addAllNewsletterInfo(queryEnv, `
      ?agenda besluitvorming:isAgendaVoor ?session .
      ?session ext:releasedDecisions ?date .
      ?session ext:heeftMailCampagnes / ext:isVerstuurdOp ?sentMailDate .

      ${extraFilters}
`);
};

const configurableQuery = function(queryString, direct, args = {}){
  const queryArgs = {
    sudo: true,
        url: direct?process.env.DIRECT_ENDPOINT:undefined
  };
  Object.assign(queryArgs,args);
  return query(queryArgs, queryString);
};

function directQuery(queryString){
  return configurableQuery(queryString, true);
};

module.exports = {
  parseSparQlResults,
  removeInfoNotInTemp,
  notConfidentialFilter,
  notInternRegeringFilter,
  notInternOverheidFilter,
  transformFilter,
  addRelatedFiles,
  cleanup,
  fillOutDetailsOnVisibleItems,
  addAllRelatedDocuments,
  addAllVisibleRelatedDocuments,
  addVisibleDecisions,
  addAllTreatments,
  addAllRelatedToAgenda,
  addVisibleNewsletterInfo,
  addAllNewsletterInfo,
  addRelatedToAgendaItemAndSubcase,
  cleanupBasedOnLineage,
  logStage,
  filterAgendaMustBeInSet,
  generateTempGraph,
  copyTempToTarget,
  configurableQuery,
  directQuery,
  runStage
};
