import mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
mu.query = querySudo;
mu.update = updateSudo;
const tempGraph = `http://mu.semte.ch/temp/${mu.uuid()}`;
const adminGraph = `http://mu.semte.ch/graphs/organizations/kanselarij`;

const addVisibleAgendas = () => {
  // TODO this will depend on the domain of the mandatee
};

