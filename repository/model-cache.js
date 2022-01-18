import { prefixes, typeUris, pathsFromAgenda } from '../model';
import { LOG_INITIALIZATION } from '../config';

export default class ModelCache {
  constructor() {
    this.pathCache = {};
    this.typeCache = [];
    this.build();
  }

  build() {
    // Building a type cache containing non-prefixed entries
    // like { key: 'agenda', uri: 'http://data.vlaanderen.be/ns/besluitvorming#Agenda' }
    this.typeCache = typeUris.map(entry => {
      const prefixedType = entry.uri;
      const parts = prefixedType.split(':');
      if (parts.length > 1) {
        const prefix = parts[0];
        const prefixUri = prefixes[prefix];

        if (prefix) {
          const resolvedType = prefixedType.replace(`${prefix}:`, prefixUri);
          return { key: entry.key, uri: resolvedType };
        } else {
          throw new Error(`No prefix definition found for '${prefix}'. Please fix the model configuration.`);
        }
      } else {
        return entry;
      }
    });
    if (LOG_INITIALIZATION)
      console.log(`Type cache: ${JSON.stringify(this.typeCache)}`);

    // Building a cache of possible property paths from an agenda to each type
    for (let key in pathsFromAgenda) {
      this.pathCache[key] = this.constructFullPaths(key);
      if (LOG_INITIALIZATION) {
        const propertyPathsForKey = this.pathCache[key].map(path => path.join(' / '));
        console.log(`Constructed paths from '${key}' to 'agenda': ${JSON.stringify(propertyPathsForKey, null, 4)}`);
      }
    }
  }

  constructFullPaths(key) {
    const entries = pathsFromAgenda[key];
    if (entries == undefined)
      throw new Error(`No pathsFromAgenda found for key '${key}. Please fix the model configuration.`);

    const fullPaths = [];
    for (let entry of entries) {
      if (entry.source) {
        // TODO first check if already available in the cache
        const parentPaths = this.constructFullPaths(entry.source);
        fullPaths.push(...parentPaths.map(path => [ ...path, entry.predicate ]));
      } else {
        fullPaths.push([ entry.predicate ]);
      }
    }
    return fullPaths;
  }

  getPathsFromAgenda(typeUri) {
    const type = this.typeCache.find(e => e.uri == typeUri);
    if (!type) {
      console.log(`Didn't find entry for type '${typeUri}'`);
      return null;
    } else if (type.key == 'agenda') {
      return [];
    } else {
      return this.pathCache[type.key];
    }
  }

}
