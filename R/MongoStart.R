# Helper function, not exported
portfree <- function(p) {!any(grepl(paste0('[^0-9]',p,' .*LISTEN'), system('netstat -an', intern = TRUE)))}
.onAttach <- function(libname, pkgname) {
  packageStartupMessage('Based on the mongolite-package by Jeroen Ooms')
}
#' MongoDB client with extra functionality
#'
#' Comparable to mongolite::mongo, but with a few extra methods (taggedfind, adjust), an extra RMongo-object,
#' and handles to the collection object and client object exposed, so you can write your own extensions
#'
#' @param collection name of collection
#' @param db name of database
#' @param url address of the mongodb server in mongo connection string \href{http://docs.mongodb.org/manual/reference/connection-string}{URI format}
#' @param host,port alternatively, you can provide a host and port, which is then resolved to "mongodb://\emph{host}[:\emph{port}]
#' Ignored if url is given. Defaults to localhost:27017
#' @param verbose emit some more output
#' @param options additional connection options such as SSL keys/certs. See mongolite documentation
#' @return A pointer to a collection on the server. It can be interfaced using methods, or the client or col attributes.
#' @section Methods:
#' The same methods as \code{\link[mongolite]{mongo}} provides, along with the following:
#' \describe{
#' \item{\code{taggedfind(qry='{}', tagfields='_id', arrayfield, sort='{}', skip=0, limit=0, handler=NULL, pagesize=1000, cachesize=5e4, verbose=verbose, stringsAsFactors=default.stringsAsFactors())}}{Find values inside documents, with pointers to the root-document, see \code{\link{taggedMongofind}}. Verbose defaults to the value given at creation of monPlus, but can be adjusted.}
#' \item{\code{adjust(findqry='{}', infields=c('All'), setfield='extraInfo',unboxsubf=c(), FUN, FUNvectorized=FALSE, skip=0, limit=0,pagesize=1000, verbose=verbose)}}{Adjust values in DB with FUN, see \code{\link{mongoAdjust}}}
#' }
#' Beside these methods, you can:
#' \describe{
#' \item{}{Access the client and collection objects themselves. In mongolite::mongo() these were hidden attributes, here you can access them directly}\cr
#' \item{}{Get an accompanying RMongo-object, as if you called RMongo::mongoDBConnect(), so you can use for example \code{dbShowCollections(monPlus('MyCol','MyDb')$Rmongo)}}
#' }
#' They are mostly useful if you want to dig deeper into the methods
#' @seealso \code{\link[mongolite]{mongo}}\cr
#' \code{\link[RMongo]{mongoDbConnect}}
#' @references \code{\link[mongolite]{mongo}}\cr
#' \href{https://jeroen.github.io/mongolite/}{Mongolite User Manual}\cr
#' Jeroen Ooms (2014). The \code{jsonlite} Package: A Practical and Consistent Mapping Between JSON Data and R Objects. \emph{arXiv:1403.2805}. \url{http://arxiv.org/abs/1403.2805}
#' @export

monPlus <- function(collection, db, url, host, port, verbose = FALSE, options = mongolite::ssl_options()) {
  if(missing(url)) {
    if(missing(host)) host <- 'localhost'
    if(missing(port)) port <- 27017
    url <- paste0('mongodb://',host,':',port)
  } else {
    host <- gsub('(^.*://)|(:[0-9]+$)','',url)
    port <- if(grepl(':[0-9]+$', url)) regmatches(url, regexpr(':[0-9]+$', url)+1) else 27017
  }
  client <- mongolite:::mongo_client_new(uri=url,options)
  col <- mongolite:::mongo_collection_new(client,collection, db)
  parent <- mongolite::mongo(collection, db, url, verbose, options)
  mlite <- new.env(parent=parent)
  mlite$client <- client
  mlite$col <- col
  mlite$RMongo <- RMongo::mongoDbConnect(db, host, port)
  verbose2 <- verbose
  mlite$taggedfind <- function(qry='{}', tagfields = "_id", arrayfield, sort = "{}", skip = 0, limit = 0, handler = NULL, pagesize = 1000, cachesize = 50000, verbose = verbose2, stringsAsFactors = default.stringsAsFactors()) {
    taggedMongofind(moncol = col, qry = qry, tagfields = tagfields, arrayfield = arrayfield, sort = sort,
                    skip = skip, limit = limit, handler=handler, pagesize = pagesize,
                    cachesize = cachesize, verbose = verbose, stringsAsFactors = stringsAsFactors)
  }
  mlite$adjust <- function(findqry = "{}", infields = c("All"), setfield = "extraInfo_from_R",
                           FUN, ..., jsonargs = list(), skip = 0, limit = 0, pagesize = 1000,
                           verbose = verbose2) {
    mongoAdjust(moncol=col, findqry = findqry, infields = infields, setfield = setfield, FUN=FUN, ...,
                jsonargs = jsonargs, skip = skip, limit = limit, pagesize = pagesize, verbose = verbose)
  }
  mlite$verbose <- verbose2
  for(f in ls(parent)) assign(f, get(f, pos=parent), mlite)
  class(mlite) <- unique(c('monPlus',class(parent), class(mlite)))
  return(mlite)
}
print.monPlus <- function(x) {
  mongolite:::print.mongo(parent.env(x))
  extra <- ls(x)[!ls(x) %in% ls(parent.env(x))]
  extracl <- sapply(extra, function(obj) {class(get(obj, x))})
  extrafun <- extra[extracl=='function']
  extraobj <- extra[extracl!='function']
  cat('Extra methods monPlus-methods:',
      paste0(' ', c(sapply(extrafun, function(obj) {
        argsline <- deparse(args(get(obj, x)))
        argsline <- argsline[-length(argsline)]
        gsub('function ', obj, argsline)
      }))),
      'Extra accessors to attributes:',
      paste0(' ',extraobj, ' (', class(get(extraobj, x)),'-object)'),
      sep='\n')
}

#' Start mongo-instance in docker
#'
#' If you want to run your own mongoDB instance, this can be done as a docker container. This function initializes an instance for you from R.
#' In a typical setup, it follows these steps:
#' \enumerate{
#'   \item Check if docker is running, if not, start it
#'   \item If update is set to TRUE, and a container with the given dockername exists, it is stopped and removed.
#'   \cr Likewise for a viewer-container, and then images are downloaded and installed.
#'   \item Check if a container with the given dockername exists. If not, create one:
#'   \itemize{
#'     \item The script first check if the given port is still free (no process is \emph{currently} listening). If yes, it uses this port.
#'     \cr If not, but the port is in use by another docker-container and kickport is set to TRUE, stop that docker-container
#'     \cr Or if port is given as a textstring ending with '+', increase portnumber until a free port is found
#'     \cr Otherwise, an error is thrown.
#'     \item if update is set to TRUE, the script checks whether a new version of the image is available, and updates that.
#'     \item Finally the docker container is started.
#'     \item If a username is given, it is initialized with the --auth option, meaning authentication is required in subsequent access as well.
#'     \item If no image-name is provided, "mongo" is used.
#'     \item Data (files in docker-fs /data/db) is stored in \emph{path}. A logfile is located here as well (called log.log)
#'   }
#'   \item If a container with the given dockername exists, but is not running, restart it.
#'   \cr In this case, the port number/imagename/path of this container is used, if needed with a warning.
#'   \item See if the docker-container is responding: if we can establish a connection, and insert and remove documents.
#'   \cr If you want to debug: it inserts and removes a document with the following content: '{ID: "ConnectionTesting_r78qfuy8asfhaksfhajklsfhajksl"}'
#'   \item if inclView is not NULL, a GUI is also initialized, as a docker container
#'   \itemize{
#'     \item Also updated if update is TRUE
#'     \item Uses port given in viewport, increased if necessary and port ends with "+"
#'     \item The docker container for the viewer is called \emph{dockername}_view. The mongo-express container can only be coupled to a mongo instance running in docker,
#'     so currently it is not possible to set up a mongo-viewer for a remote server.
#'     \item If there is already a container with that name, it is restarted (note that this cannot be the case if update was TRUE, then any viewer-container is already removed).
#'     \cr If this container is pointing to another mongo-instance, a warning is generated.
#'   }
#'   \cr That GUI can be found with any internet browser, going to http://localhost:\emph{viewport}
#' }
#' You can skip any of these steps with the skip parameter, e.g. if you already know you want to connect to a certain port,
#' but don't know what the docker container is called (or there is no docker container, and you want to connect to a remote server).
#' \cr\cr Furthermore, the starting process of docker and the docker container may take some time. This can be done in the background, by setting preOnly to TRUE.
#' If the script then encounters a situation in which it has to wait, it returns a numerical, indicating the step to resume. Thus, you can run your own script by first calling
#' OpenDockerMongo(preOnly=TRUE, ...), then soing some useful work (that may take time), then calling OpenDockerMongo again (when it will hopefully be ready for the next step)
#'
#' @param dockername name of docker container that is runnning the mongo-server. May be NULL if skip >1, in which case it connects to the host and port given.
#' @param imagename name of docker image to use. If dockername is given, a container already exists and update is FALSE, this is ignored (if needed with a warning, supply NULL to also suppress warnings)
#' @param path path to store data (docker file-system: /data/db). If dockername is given, a container already exists and update is FALSE, this is ignored (if needed with a warning, this may cause false positives if symbolic links are used, or a path needs to be further expanded)
#' /cr If path does not exist, it is created, and a new database is initialized.
#' @param host use to connect to a remote server. In this case, skip is set to 4, any docker parameters are ignored (with a warning if needed)
#' @param port port to use, default mongoport is 27017. Appending with "+" means a higher port number may be used if needed. Ignored if a container is just restarted (with a warning if needed, use port 0 to suppress any warnings)
#' @param kickport If port is already in use by another docker container, should this container be stopped? Ignored if port ends in "+". Only looks at running containers, and stops them, but doesn't remove them.
#' @param inclView also initialize an extra container that links to the mongo-server, for example to be used as a GUI. The default, mongo-express, gives a webinterface to the mongo-DB.
#' \cr Given as a string with the docker imagename, 'previous', or NULL to not start anything. The value 'previous' only works if there already is a viewer-container initialized, which you just want to restart without any checks.
#' \cr Currently only tested with NULL or mongo-express, unlikely to work for other vallues (but may be expanded in the future).
#' @param viewport Port to use for the viewer-container, can also be appended with "+", ignored if inclView==FALSE
#' @param update logical, should the script restart everything? If TRUE does the following:\itemize{
#'   \item stop containers with \emph{dockername} or \emph{dockername_view}
#'   \item remove containers with \emph{dockername} or \emph{dockername_view}
#'   \item Update the docker images (imagename and inclView if not NULL)
#' }
#' @param preOnly logical, if the script has to wait for a docker action, should it return control? If TRUE and it has to wait, it returns a numerical that can be given to skip for the next call.
#' Does not work for downloading new images (cannot be run as background task)
#' \cr Note that if the return is >2, any old containers are already removed and updated, so setting update to TRUE would waste extra time.
#' @param skip steps to skip, e.g. if you already have a server instance initialized, you can use skip=4. Numbering is identical to the steps enumerated here, the script starts with skip+1.
#' @param db,collection,user,pswd Parameters used for connecting to the server: databasename, collection-name, username and password. If user is NULL,
#' authentication is disabled when creating the container.
#' @param verbose print output indicating status?
#'
#' @return An monPlus-object, or if preOnly is TRUE and we have to wait for a background process, a numerical indicating what step we are (which can be given to skip).
#' @examples
#'   # From a fresh install.
#'   # Using preOnly means that while docker is starting, control is returned.
#'   DB <- OpenDockerMongo('MyMongoContainer', path='~/Docker/Mongo/MyDb',
#'   preOnly=TRUE, db='MyDb',collection='MyCol')
#'   # Generate new documents to insert, while generating these docs,
#'   # docker processes are running in the background:
#'   Docs <- data.frame(MyID=1:100, myData=rnorm(100))
#'   if(is.numeric(DB)) DB <- OpenDockerMongo('MyMongoContainer', path='~/Docker/Mongo/MyDb',
#'   db='MyDb',collection='MyCol', skip=DB)
#'   # Control is only returned when finished, so DB is now a monPlus-object.
#'   DB$insert(Docs)
#'   # If port 8081 was previously free, you can now browse your docs at http://localhost:8081
#'
#'   # Cleaning up:
#'   DB$remove(paste0('{"MyID": {"$in": [',paste0(1:100, collapse=', '),']}}'))
#'   if(DB$count()==0) DB$drop()
#'   system('docker stop MyMongoContainer')
#'   system('docker stop MyMongoContainer_view')
#'   system('docker rm MyMongoContainer')
#'   system('docker rm MyMongoContainer_view')
#'
#' @seealso \url{http://www.docker.com} for general information on docker
#' \cr \url{https://hub.docker.com/_/mongo/} for information on running a mongo-container in docker
#' \cr \pkg{mongolite}
#' \cr \pkg{RMongo}
#' @export

OpenDockerMongo <- function(dockername, imagename='mongo', path,
                host='localhost', port='27017+', kickport=FALSE,
                inclView='mongo-express', viewport='8081+', update=FALSE, preOnly=FALSE, skip=0,
                db, collection, user=NULL, pswd=NULL, verbose=TRUE) {
  if(!is.numeric(port)) {
    if(!grepl('\\+$', port)) stop('Bad arguments: Unclear port')
    port <- suppressWarnings(as.numeric(gsub('\\+$','',port)))
    if(is.null(port) || is.na(port) || !is.numeric(port) || port==0) stop('Unclear port')
    plusport <- TRUE
  } else plusport <- FALSE
  if(!is.numeric(viewport)) {
    if(!grepl('\\+$', viewport)) stop('Bad arguments: Unclear viewport')
    viewport <- suppressWarnings(as.numeric(gsub('\\+$','',viewport)))
    if(is.null(viewport) || is.na(viewport) || !is.numeric(viewport) || viewport==0) stop('Unclear viewport')
    plusviewport <- TRUE
  } else plusviewport <- FALSE
  if(!missing(path)) path <- path.expand(path)
  if(!Sys.info()['sysname'] %in% c('Darwin'))
    warning('This function has only been tested on OSX, system calls may not function on your system.')
  if(skip<4 && !missing(host) && !host %in% c('localhost','127.0.0.1','::1')) {
    if(!missing(dockername) && !is.null(dockername) ||
       !missing(imagename) && !is.null(imagename) && imagename!='mongo' ||
       !missing(path) && !is.null(path) ||
       !is.numeric(port) ||
       !missing(inclView) && !is.null(inclView) && inclView!='mongo-express' ||
       !missing(viewport) && !is.null(viewport) && viewport!='8081+')
      warning('You have provided a remote host (',host,'), but parameters which suggest running a docker-container, which is incomaptible.\n',
              'Ignoring parameters, trying to just set up connection instead')
    skip <- 4
    inclView <- NULL
  }
  if(skip<1) { # Check if docker is running
    if(!any(grepl('Docker.app', system('/bin/ps aux', intern=TRUE)))) {
      if(verbose) cat('Starting docker')
      if((retcode <- system('open --background -a Docker'))!=0) stop('Unxpected return value when starting docker:\n',retcode)
      if(preOnly) return(0)
    }
    # Tries: a counter that keeps track of how often we've already tried to connect to docker.
    # Special value: 1e6=no success, but preOnly, so we have to return. 2e6+n: Success after n attempts
    tries <- 0
    while(tries<500) {
      tryCatch({
        if(substring(suppressWarnings(system2('docker', args=c('ps'), stderr=TRUE, stdout=TRUE)[1]),
                     1,12)!='CONTAINER ID') stop('ToCatchError')
        tries <- tries+2e6
      }, error=function(e) {
        if(preOnly) {
          tries <- 1e6
        } else {
          cat('.')
          tries <<- tries+1
          Sys.sleep(tries<500)
        }
      })
    }
    if(tries==1e6) return(0)
    if(tries==500) {
      stop('\nDocker seems not to get ready')
    } else if(verbose && tries>2e6) {
      cat('\nDocker started succesfully.')
    } else {
      cat('Docker is running and responding.')
    }
    cat('\n')
  } # Check if docker is running
  if(skip>1 && update) {
    warning('OpenDockerMongo: Restarting docker containers (argument update set to TRUE) means going back to step 2, which is incompatible with skip=',skip,
            '.\nIgnoring this skip value.')
    skip <- 1
  }
  if(skip<2 && update) { # Remove any existing/running containers
    for(name in c(paste0(dockername, '_view'), dockername)) {
      if(any(grepl(paste0(' ',gsub('.','\\.',name, fixed=TRUE),'$'),system('docker ps', intern=TRUE)))) {
        if(verbose) cat('Stopping container "',name,'"\n', sep='')
        if(system(paste('docker stop', name), intern=TRUE)!=name)
          stop('Unexpected retun value from docker engine when attempting to stop ', name)
      }
      if(any(grepl(paste0(' ',gsub('.','\\.',name, fixed=TRUE),'$'),system('docker ps -a', intern=TRUE)))) {
        if(verbose) cat('Removing container "',name,'"\n', sep='')
        if(system(paste('docker rm', name), intern=TRUE)!=name)
          stop('Unexpected retun value from docker engine when attempting to stop ', name)
      }
    }
  } # Remove any existing/running containers
  if(skip<3) { # Check if container exists, if not, create one
    if(missing(dockername)) stop('No dockername supplied, but this is needed unless skip>=3')
    if(!any(grepl(paste0(' ',gsub('.','\\.',dockername, fixed=TRUE),'$'),system('docker ps -a', intern=TRUE)))) {
      # Do we need to update the image/get a new image?
      if(update || !any(grepl(paste0('^',gsub(':',' +',imagename),' '), system('docker image ls', intern=TRUE)))) {
        if(verbose) cat('Pulling image, this might take a while.\n')
        retval <- system(paste('docker pull', imagename), intern = TRUE)
        if(!is.null(attr(retval,'status')) ||
           !grepl(paste0('(Status: Image is up to date for ',imagename,')|(Status: Downloaded newer image for ',imagename,')'),
                  retval[length(retval)]))
          stop('Error in pulling new image. Output:\n',
               paste(retval, sep = '\n'),'\n')
        if(verbose) cat(gsub('^Status: ','',retval[length(retval)]),'\n')
      }
      if(verbose) cat('Creating mongo-container "',dockername,'"\n', sep = '')
      if(missing(path)) stop('When a new dockercontainer needs to be created, path must be supplied')
      if(!plusport && !kickport && !portfree(port)) warning('It looks like port',port,'is already in use. Continuing, but this function will likely fail.')
      while(plusport && !portfree(port+is.numeric(plusport)*plusport)) plusport <- is.numeric(plusport)*plusport+1
      port <- port+is.numeric(plusport)*plusport
      if(verbose && is.numeric(plusport)) cat('Using port',port,'for mongo-container\n')
      if(kickport && !portfree(port)) { # If both plusport and kickport were TRUE, portfree(port) already gives TRUE at this point
        retval <- system('docker ps', intern=TRUE)[-1]
        tostop <- retval[grepl(paste0(':',port,'->27017/tcp'), retval)]
        tostop <- substr(tostop, regexpr('[^ ]+$', tostop), nchar(tostop))
        tostop <- tostop[!is.na(tostop) & !is.null(tostop) & tostop!='']
        if(length(tostop)>1) stop('Error in trying to decide which container to stop')
        if(length(tostop)==0) stop('Error in freeing port ',port, '. Is this port in use by a non-docker process?')
        if(verbose) cat('Freeing port ',port,' by stopping docker-container with name ',tostop,'.\n',sep = '')
        if(system(paste('docker stop',tostop), intern=TRUE)!=tostop) stop('Unexpected return value when trying to stop ',tostop)
      }
      retval <- suppressWarnings(system2(command='docker', args=paste0(
          'run --name ',dockername,' -v ',path,':/data/db -p ',port,':27017 -d '
          ,imagename, ' --logpath /data/db/log.log',ifelse(is.null(user),'',' --auth')),
        stdout = TRUE, stderr=TRUE))
      if(!is.null(attr(retval,'status')) || length(retval)>1 || grepl('[^0-9a-fA-F]', retval))
        stop('Starting docker-container seemed unsuccesful. Extra details:\n',
             'Command run:\n',
             paste0('docker run --name ',dockername,' -v ',path,':/data/db -p ',port,':27017 -d '
                    ,imagename, ' --logpath /data/db/log.log',ifelse(is.null(user),'',' --auth'),'\n'),
             'Return status-code: ',ifelse(is.null(attr(retval,'status')), '0 (success)',attr(retval,'status')),'\n',
             'Return text:\n',
             paste(retval, collapse='\n'),'\n'
        )
      if(verbose) cat('Docker-container "', dockername, '" created succesfully.\n', sep='')
    }
  } # Check if container exists, if not, create one
  if(skip<4) { # If container exists but is not running, restart it. And do some checks
    if(!any(grepl(paste0(' ',gsub('.','\\.',dockername, fixed=TRUE),'$'),system('docker ps', intern=TRUE)))) {
      if(!any(grepl(paste0(' ',gsub('.','\\.',dockername, fixed=TRUE),'$'),system('docker ps -a', intern=TRUE))))
        stop('No container found with name "',dockername,'", which is unexpected at this step')
      retval <- system(paste('docker start', dockername), intern=TRUE)
      if(!is.null(attr(retval,'status')) || length(retval)!=1 || retval!=dockername)
        stop('Restarting container "', dockername, '" seemed unsuccesful. Details:\n',
             'Command used: docker start ', dockername, '\n',
             'Return code: ', ifelse(is.null(attr(retval, 'status')), '0 (success)', attr(retval, 'status')), '\n',
             'Return text:\n',
             paste(retval, collapse='\n'))
      if(verbose) cat('Restarted container "',dockername,'"\n', sep='')
    } else {
      cat('Found running docker container.\n')
    }
    retval <- system(paste('docker inspect', dockername), intern=TRUE)
    if(!is.null(attr(retval, 'status'))) stop('Inspecting of docker container "',dockername,'" failed.')
    # Checks: imagename, path, port
    if(!is.null(imagename) && !any(grepl(paste0('"Image": "',imagename,'"'), retval, fixed=TRUE)))
      warning('Restarted docker-container "',dockername,'", but this container is not running image ',imagename)

    contpath <- regexpr('"Source": "[^"]+",', gsub('\\','/',retval, fixed=TRUE))
    contpath[contpath!=-1][!grepl('"Destination": "/data/db",',retval[which(contpath!=-1)+1])] <- -1
    if(sum(contpath!=-1)!=1) stop('Error in trying to find this containers path.')
    contpath <- substring(retval[contpath!=-1], contpath[contpath!=-1]+11,
                          contpath[contpath!=-1]+attr(contpath, 'match.length')[contpath!=-1]-3)
    if(!missing(path) && contpath!=path)
      warning('Restarted docker-container "',dockername,'", but this container is using path "',contpath,
              '" instead of the path provided ("',path,'")')
    path <- contpath

    contport <- regexpr('"HostPort": "[^"]+"', retval)
    contport[contport!=-1][!grepl('"27017/tcp": [',retval[which(contport!=-1)-3], fixed=TRUE)] <- -1
    if(sum(contport!=-1)>1 && all(retval[contport!=-1][-1]==retval[contport!=-1][1])) contport[contport!=-1][-1] <- -1
    if(sum(contport!=-1)!=1) stop('Error in trying to find this containers path.')
    contport <- substring(retval[contport!=-1], contport[contport!=-1]+13,
                          contport[contport!=-1]+attr(contport, 'match.length')[contport!=-1]-2)
    contport <- suppressWarnings(as.numeric(contport))
    if(is.null(contport) || is.na(contport) || contport==0) stop('Error in trying to find this containers port number.')
    if(port!=0 && port!=contport)
      warning('Restarted docker-container "',dockername,'", but this container is using port ',contport,
              ' instead of the port provided (',port,')')
    port <- contport
  } # If container exists but is not running, restart it. And do some checks
  if(skip<5) { # See if mongo-engine is running
    if(!any(grepl(paste0('Connection to.* port ',port,'.*succeeded!'),
                  suppressWarnings(system2('nc', args=c('-z',host,port), stdout=TRUE, stderr=TRUE, timeout=5)))))
      stop('No connection could be established to host ',host,' on port ', port)
    tries <- 0 # Special value: 1e6 means failure to connect, but preOnly. 2e6+n is success after n+1 attempts
    if(verbose) cat('Trying to establish connection')
    while(tries<100) {
      tryCatch(suppressWarnings({
        Rmon <- RMongo::mongoDbConnect(db, host, port)
        RMongo::dbInsertDocument(Rmon, collection, '{ID: "ConnectionTesting_r78qfuy8asfhaksfhajklsfhajksl"}')
        if(nrow(RMongo::dbGetQuery(Rmon, collection, '{}', skip=0, limit=10))<1)  stop('ToCatchError')
        if(RMongo::dbRemoveQuery(Rmon, collection, '{ID: "ConnectionTesting_r78qfuy8asfhaksfhajklsfhajksl"}')!='ok') stop('ToCatchError')
        tries <- tries+2e6
      }), error=function(e) {
        if(preOnly) {
          tries <- 1e6
        } else {
          cat('.')
          tries <<- tries+1
          Sys.sleep(tries<100)
        }
      })
    }
    if(tries==1e6) return(4)
    if(tries==100) {
      stop('\nMongo-engine seems not to get ready.')
    } else if(verbose && tries>=2e6) {
      cat('\nSuccesfully connected.')
    }
    cat('\n')
  } # See if mongo-engine is running
  if(skip<6 && !is.null(inclView)) {
    if(missing(dockername)) stop('A viewer can only be included if running mongo in a docker-container, but no dockername is specified')
    if(!is.character(inclView) || !inclView %in% c('mongo-express','previous')) stop('Currently, only the mongo-express viewer is supported')
    # First step: does a container already exist?
    if(!any(grepl(paste0(' ',gsub('.','\\.',dockername, fixed=TRUE),'_view$'),system('docker ps -a', intern=TRUE)))) {
      if(inclView=='previous') stop('inclView=="previous" implies a container with the name "', dockername, '_view" already exists, but this is not the case.')
      # Do we need to update the image/get a new image?
      if(update || !any(grepl(paste0('^',gsub(':',' +',inclView),' '), system('docker image ls', intern=TRUE)))) {
        if(verbose) cat('Pulling image ', inclView, ', this might take a while.\n', sep='')
        retval <- system(paste('docker pull', inclView), intern = TRUE)
        if(!is.null(attr(retval,'status')) ||
           !grepl(paste0('(Status: Image is up to date for ',inclView,')|(Status: Downloaded newer image for ',inclView,')'),
                  retval[length(retval)]))
          stop('Error in pulling new image. Output:\n',
               paste(retval, sep = '\n'),'\n')
        if(verbose) cat(gsub('^Status: ','',retval[length(retval)]),'\n')
      }
      if(verbose) cat('Creating mongo-container "',dockername,'"\n', sep='')
      if(skip>=4) {
        # Then we still need to check/decide input
        # First image:
        retval <- system(paste('docker inspect',dockername), intern=TRUE)
        contimagename <- regexpr('"Image": "[^"]+"', retval)
        contimagename[!cumsum(grepl('"Config": \\{$', retval))] <- -1 # Only select images named after the Config header
        if(sum(contimagename!=-1)!=1) stop('Error in trying to find imagename of container "', dockername,'"')
        contimagename <- substring(retval[contimagename!=-1], contimagename[contimagename!=-1]+10,
                                   contimagename[contimagename!=-1]+attr(contimagename, 'match.length')[contimagename!=-1]-2)
        if(!is.null(imagename) && contimagename!=imagename)
          warning('Docker-container "', dockername, '" is running "', contimagename, '" image, instead of "',
                  imagename, '" as was supplied. Provided imagename will be ignored.')
        imagename <- contimagename
        # Path is no consequence to viewer, so next is port:
        contport <- regexpr('"HostPort": "[^"]+"', retval)
        contport[contport!=-1][!grepl('"27017/tcp": [',retval[which(contport!=-1)-3], fixed=TRUE)] <- -1
        if(sum(contport!=-1)>1 && all(retval[contport!=-1][-1]==retval[contport!=-1][1])) contport[contport!=-1][-1] <- -1
        if(sum(contport!=-1)!=1) stop('Error in trying to find this containers path.')
        contport <- substring(retval[contport!=-1], contport[contport!=-1]+13,
                              contport[contport!=-1]+attr(contport, 'match.length')[contport!=-1]-2)
        contport <- suppressWarnings(as.numeric(contport))
        if(is.null(contport) || is.na(contport) || contport==0) stop('Error in trying to find this containers port number.')
        if(port!=0 && port!=contport)
          warning('Docker-container "',dockername,'" is mapped to port ',contport,
                  ' instead of the port provided (',port,'). Provided portnumber will be ignored')
        port <- contport
      }
      if(verbose) cat('Creating mongo-container "',dockername,'_view"\n', sep='')
      if(!plusviewport && !kickport && !portfree(viewport)) warning('It looks like port',viewport,'is already in use. Continuing, but this function will likely fail.')
      while(plusviewport && !portfree(viewport+is.numeric(plusviewport)*plusviewport)) plusviewport <- is.numeric(plusviewport)*plusviewport+1
      viewport <- viewport+is.numeric(plusviewport)*plusviewport
      if(verbose && is.numeric(plusviewport)) cat('Using port',viewport,'for mongo-viewer-container\n')
      if(kickport && !portfree(viewport)) { # If both plusviewport and kickport were TRUE, portfree(viewport) already gives TRUE at this point
        retval <- system('docker ps', intern=TRUE)[-1]
        tostop <- retval[grepl(paste0(':',viewport,'->8081/tcp'), retval)]
        tostop <- substr(tostop, regexpr('[^ ]+$', tostop), nchar(tostop))
        tostop <- tostop[!is.na(tostop) & !is.null(tostop) & tostop!='']
        if(length(tostop)>1) stop('Error in trying to decide which container to stop')
        if(length(tostop)==0) stop('Error in freeing port ',viewport, '. Is this port in use by a non-docker process?')
        if(verbose) cat('Freeing port ',viewport,' by stopping docker-container with name "',tostop,'".\n',sep = '')
        if(system(paste('docker stop',tostop), intern=TRUE)!=tostop) stop('Unexpected return value when trying to stop "',tostop,'".')
      }

      retval <- suppressWarnings(system2(command='docker', args=paste0(
          'run --link ',dockername,':',imagename,' -p ',viewport,':8081 --name ',dockername, '_view ',
          if(!is.null(user)) paste0('-e ME_CONFIG_MONGODB_ADMINUSERNAME="',user,
                                    '" -e ME_CONFIG_MONGODB_ADMINPASSWORD="', pswd,
                                    '" -e ME_CONFIG_BASICAUTH_USERNAME="', user,
                                    '" -e ME_CONFIG_BASICAUTH_PASSWORD="', pswd,'" '),
          '-d mongo-express'),
        stdout = TRUE, stderr=TRUE))
      if(!is.null(attr(retval,'status')) || length(retval)>1 || grepl('[^0-9a-fA-F]', retval)) {
        stop('Starting docker-container seemed unsuccesful. Extra details:\n',
             'Command run:\n\tdocker ',
             paste0('run --link ',dockername,':',imagename,' -p ',viewport,':8081 --name ',dockername, '_view ',
                    if(!is.null(user)) paste0('-e ME_CONFIG_MONGODB_ADMINUSERNAME="',user,
                                              '" -e ME_CONFIG_MONGODB_ADMINPASSWORD="', pswd,
                                              '" -e ME_CONFIG_BASICAUTH_USERNAME="', user,
                                              '" -e ME_CONFIG_BASICAUTH_PASSWORD="', pswd,'" '),
                    '-d mongo-express'),
             '\nReturn status-code: ',ifelse(is.null(attr(retval,'status')), '0 (success)',attr(retval,'status')),'\n',
             'Return text:\n',
             paste('\t', retval[1:min(10,length(retval))], collapse='\n'),'\n',
             if(length(retval)>10) paste('(truncated', length(retval)-10, 'lines)'))
      }
      if(verbose) cat('Docker-container "', dockername, '_view" created succesfully.\n', sep='')
    }
    # Do we need to restart?
    if(!any(grepl(paste0(' ',gsub('.','\\.',dockername, fixed=TRUE),'_view$'),system('docker ps', intern=TRUE)))) {
      if(!any(grepl(paste0(' ',gsub('.','\\.',dockername, fixed=TRUE),'_view$'),system('docker ps -a', intern=TRUE))))
        stop('No container found with name "',dockername,'_view", which is unexpected at this step')
      retval <- system(paste0('docker start ', dockername,'_view'), intern=TRUE)
      if(!is.null(attr(retval,'status')) || length(retval)!=1 || retval!=paste0(dockername,'_view'))
        stop('Restarting container "', dockername, '_view" seemed unsuccesful. Details:\n',
             'Command used: docker start ', dockername, '_view\n',
             'Return code: ', ifelse(is.null(attr(retval, 'status')), '0 (success)', attr(retval, 'status')), '\n',
             'Return text:\n',
             paste(retval, collapse='\n'))
      if(verbose) cat('Restarted container "',dockername,'_view"\n', sep='')
    }
    # Checks: pointing to right mongo-instance?
    retval <- system(paste0('docker inspect ', dockername, '_view'), intern=TRUE)
    if(!is.null(attr(retval, 'status'))) stop('Inspecting of docker container "',dockername,'_view" failed.')
    # Checks: imagename, port, docker-linkage
    if(inclView!='previous') {
      if(!any(grepl(paste0('"Image": "',inclView,'"'), retval, fixed=TRUE)))
        warning('Restarted docker-container "',dockername,'_view", but this container is not running image ',inclView)
    }
    contport <- regexpr('"HostPort": "[^"]+"', retval)
    contport[contport!=-1][!grepl('"8081/tcp": [',retval[which(contport!=-1)-3], fixed=TRUE)] <- -1
    if(sum(contport!=-1)>1 && all(retval[contport!=-1][-1]==retval[contport!=-1][1])) contport[contport!=-1][-1] <- -1
    if(sum(contport!=-1)!=1) stop('Error in trying to find this containers path.')
    contport <- substring(retval[contport!=-1], contport[contport!=-1]+13,
                          contport[contport!=-1]+attr(contport, 'match.length')[contport!=-1]-2)
    contport <- suppressWarnings(as.numeric(contport))
    if(is.null(contport) || is.na(contport) || contport==0) stop('Error in trying to find this containers port number.')
    if(viewport!=0 && viewport!=contport)
      warning('Restarted docker-container "',dockername,'", but this container is using port ',contport,
              ' instead of the port provided (',viewport,')')
    viewport <- contport

    linked <- grep('"Links": [', retval, fixed=TRUE)
    if(length(linked)>1) stop('Error in trying to extract linking-information from container "',dockername,'_view"')
    linked <- retval[(linked+1):(linked+grep('],$', retval[linked:length(retval)])[1]-2)]
    linkedcont <- gsub(paste0('(^[^"]+"/)|(:/',dockername, '_view.*)'),'',linked)
    linkedimg <- gsub(paste0('(^.*',dockername,'_view/)|("$)'),'',linked)
    if(dockername!=linkedcont)
      warning('Viewer is linked to container "', linkedcont, '" instead of "',dockername,'"')
    if(!is.null(imagename) && imagename!=linkedimg)
      warning('Viewer is linked to image "', linkedimg, '" instead of "',imagename,'"')
  }
  return(monPlus(collection = collection, db = db, host = host, port=port, verbose = verbose))
}



























