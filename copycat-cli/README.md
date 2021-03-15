# CopyCat CLI

The CLI of CopyCat allows the deduplication of run and qrel-files and comes with a docker image with support for jupyter notebooks (+ [common data science libraries](https://hub.docker.com/r/jupyter/datascience-notebook/)).

## Getting Started

The docker image provides an easy way to run CopyCat. Start run the CopyCat cli with docker, run: 
```
docker run --rm -ti -v ${PWD}:/home/jovyan webis/chatnoir-copycat:1.0-jupyter copy-cat --help
```

This command will print the help to the console.

Similar, you can start a jupyter notebook with CopyCat installed:
```
docker run --rm -ti -v ${PWD}:/home/jovyan -p 8888:8888 webis/chatnoir-copycat:1.0-jupyter
```

## Development Environment

The CLI of CopyCat uses maven as build tool.
Please ensure that all maven dependencies are located in your local maven repository by running `make install` in the [root of the repository](..).
To develop in Eclipse, install the [lombok](https://projectlombok.org/) plugin.
Then import this project as "Existing Maven Project" to Eclipse.

