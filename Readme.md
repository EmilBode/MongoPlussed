# MongoPlussed
Extensions of Mongolite Package for R

Written by Emil Bode, data-analyst at DANS.
Extra interfaces for working with a mongo-database. It is largely based on the mongolite package by Jeroen Ooms, but contains some extensions: Script for creating and accessing mongo-dbs in a docker-container, and for simplifying querying in nested documents. And it hides less of the internals, so you can more easily write own extensions, and also use methods from the RMongo-packaage.
For more information, see also the DESCRIPTION-file.

This package is stored in 2 places: at Dans-labs is the more stable version, at my peronsal github (EmilBode) is a development-version (alpha)
To install: devtools::install_github('Dans-labs/MongoPlussed') or devtools::install_github('EmilBode/MongoPlussed') for the development-version
