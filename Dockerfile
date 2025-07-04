FROM semtech/mu-javascript-template:latest
LABEL maintainer=info@redpencil.io

 # overwrite default of mu-javascript-template
ENV LOG_SPARQL_ALL "false"
