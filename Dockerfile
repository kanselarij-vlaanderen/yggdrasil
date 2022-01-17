FROM semtech/mu-javascript-template:1.5.0-beta.4
LABEL maintainer=info@redpencil.io

ENV LOG_INCOMING_DELTA="false"
ENV LOG_INITIALIZATION="false"
ENV LOG_DELTA_PROCESSING="true"
ENV LOG_SPARQL_ALL="false"
