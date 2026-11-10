FROM semtech/mu-javascript-template:feature-draft-1.10.0-clean-slim
LABEL maintainer=info@redpencil.io

 # overwrite default of mu-javascript-template
ENV LOG_SPARQL_ALL="false"
