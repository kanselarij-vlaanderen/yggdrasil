FROM semtech/mu-javascript-template:v1.5.0-beta.1
LABEL maintainer=info@redpencil.io

ENV LOG_INCOMING_DELTA="false"
ENV LOG_INITIALIZATION="false"
ENV LOG_DELTA_PROCESSING="true"
ENV LOG_SPARQL_ALL="false"
