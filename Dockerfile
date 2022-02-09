FROM semtech/mu-javascript-template:1.5.0-beta.4
LABEL maintainer=info@redpencil.io

 # overwrite default of mu-javascript-template
ENV LOG_SPARQL_ALL "false"
