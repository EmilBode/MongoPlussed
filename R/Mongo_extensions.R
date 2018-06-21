#' Simplified find in mongo
#'
#' When querying a mongo-database and getting back a large result-set, most time is spent on casting this to a data.frame.\cr
#' Especially when this is a intermediate step, it is also often unnecessary.
#' So this function does as little as needed, to save time.\cr
#' Also useful if for some reason casting to dataframe produces unexpected results.\cr
#' This function was written before I found out about iterate(), so unclear if it has much use
#' Note that the original mongolite find-function was more object-oriented, and used a mongo connection object, while this function
#' has to be supplied a mongo-collection object (in mongolite, this is stored as an internal pointer). You can get this by connecting using monPlus, and then accessing the 'col' method.
#' Or simply accessing this function with the $simfind-method
#' @param moncol mongo-collection pointer
#' @param qry a query to be supplied to the mongo-database, in json-notation
#' @param fields fields to be retrieved, in json-notation
#' @param sort fields to be sorted on
#' @param skip number of records to skip. Useful together with limit, to continue later
#' @param limit limit to the number of records returned, or 0 for no limit
#' @param handler A function to be applied over the results, e.g. to further filter results. Note that it differs from a handler given to regular "find" in that it is not given a data-frame.
#' @param pagesize size of paging to use, see return value
#' @param verbose emit extra output in the form of a counter
#' @return A list of pages with the returns from the mongo DB. To get to a simple list of returns, use unlist(recursive=FALSE)
#' @seealso mongolite::mongo
#'
simpleMongofind <- function(moncol, qry='{}', fields = '{"_id" : 0}', sort = '{}', skip = 0, limit = 0, handler = NULL, pagesize = 1000, verbose=F) {
  warning('Deprecated, use iterate() instead')
  if(class(moncol)!='mongo_collection' || mongolite:::null_ptr(moncol)) stop('Mongo collection pointer is invalid or dead')
  cur <- mongolite:::mongo_collection_find(moncol, query = qry, sort = sort, fields = fields, skip = skip, limit = limit)
  stopifnot(is.null(handler)||is.function(handler))
  cnt <- 0
  cb <- {
    out <- new.env()
    if(is.null(handler)) {
      function(x){
        if(length(x)){
          cnt <<- cnt + length(x)
          out[[as.character(cnt)]] <<- x
        }
      }
    } else {
      function(x){
        if(length(x)){
          cnt <<- cnt + length(x)
          x <- handler(x)
          out[[as.character(cnt)]] <<- x
        }
      }
    }
  }
  repeat {
    page <- mongolite:::mongo_cursor_next_page(cur, pagesize)
    if(length(page)){
      cb(page)
      if(verbose) cat("\rFound", cnt, "records...")
    }
    if(length(page) < pagesize) {
      if(verbose) cat('\n')
      break
    }
  }
  return(as.list(out))
}


#' Find values inside documents, with pointers to the root-document.
#'
#' Try to find values in an array in different documents, and list them. You can also add tagfields, useful as pointers to which documents they are coming from.
#' Say, for example, you have a database of scientific articles, each with multiple authors, and you want a list of authors, along with which documents they have produced.
#' This gives back a list of these authors, along with identifier-fields for each document
#' @param moncol mongo-collection pointer. This can be obtained with YourMonplusObject$col, or call this function via your monplus-object
#' @param qry Query to run to decide what documents to return. This is on document-level, so if you query {"Author": {"LastName": "Smith"}}, you'll also get Smiths co-authors. To delete this, you can either subset later, or provide a handler
#' @param tagfields Field(s) to return as identifier labels. These should be top-level values
#' @param arrayfield One field that is an array. This may have multiple subfields, e.g. author having first name and last name. The results are combined into one dataframe along with tagfields.
#' Nested fields are seperated by dots.
#' @param sort Sorting to use, as a JSON object (passed on to Mongo-engine)
#' @param skip number of records to skip. Useful together with limit, to continue later
#' @param limit limit to the number of records returned, or 0 for no limit
#' @param handler handler to further process results. Results are first transformed to a data.frame, then given to the handler, per record.
#' @param pagesize size of page to use when querying mongo-engine
#' @param cachesize size of intermediate cache to use. Set this for debugging purposes, may be removed in the future
#' @param verbose emit extra output (counter)
#' @param stringsAsFactors logical: should character vectors be converted to factors? The ‘factory-fresh’ default is TRUE, but this can be changed by setting options(stringsAsFactors = FALSE).
#' @return A dataframe, consisting of the fields in arrayfield, and (repeated) top-level document fields. The top-level fields are prefixed with "rec_"
#' @examples
#' # Assumed: we can establish a connection to mongodb://localhost:27017,
#' # and that this is a new or empty database/collection
#' MyMongo <- monPlus('MyCol','MyMon')
#' MyMongo$insert(c('{"OwnID":"Doc1","Authors": {"Name": ["John Smith", "Ben"], "RegistrNo": [1,2]}}',
#' '{"OwnID":"Doc2","Authors": {"Name": ["John Smith", "William Smith"], "RegistrNo": [3,4]}}',
#' '{"OwnID":"Doc3","Authors": {"Name": ["William", "Benjamin Smith"], "RegistrNo": [4,2]}}'))
#' taggedMongofind(MyMongo$col, '{"Authors.Name": "John Smith"}',
#' arrayfield = 'Authors', tagfields = 'OwnID')
#' \dontshow{MyMongo$remove('{"OwnID": {"$in": ["Doc1","Doc2","Doc3"]}}')
#' if(MyMongo$count()==0) MyMongo$drop()}
#' @export
taggedMongofind <- function(moncol, qry='{}', tagfields='_id', arrayfield, sort='{}', skip=0, limit=0, handler=NULL, pagesize=1000, cachesize=5e4, verbose=FALSE, stringsAsFactors=default.stringsAsFactors()) {
  if(class(moncol)!='mongo_collection' || mongolite:::null_ptr(moncol)) stop('Mongo collection pointer is invalid or dead')
  if(is.null(arrayfield) || is.na(arrayfield) || !is.character(arrayfield) || length(arrayfield)<1) stop('No valid arrayfield specified')
  if(length(arrayfield)>1) {
    warning('Arrayfield length is >1, only first one will be used')
    arrayfield <- arrayfield[1]
  }
  cur <- mongolite:::mongo_collection_find(moncol, query = qry, sort = sort, skip = skip, limit = limit, fields=paste0(
    '{"',paste0(c(tagfields, arrayfield), collapse = '":true, "'),'":true', ifelse('_id' %in% tagfields, '}', ', "_id":0}')))
  cnt <- 0
  cache <- data.frame()
  result <- data.frame()
  bind_rows <- dplyr::bind_rows
  simple_rapply <- EmilMisc::simple_rapply
  flat <- function(x) {
    x <- if(is.null(x) || length(x)==0) NA else x
    if(is.null(names(x)) && class(x)=='list' && class(x[[1]])!='list' &&
       all(sapply(x, class)==class(x[[1]])) && all(sapply(x, length)==1)) {
      x <- unlist(x)
    }
    if(!is.null(names(x)) && class(x)=='list' && all(sapply(x, is.atomic)) &&
       all(sapply(x, length)==length(x[[1]]))) {
      x <- do.call(data.frame, args=c(x, stringsAsFactors = FALSE))
    }
    if(class(x)=='list' && all(sapply(x, class)=='data.frame')) {
      x <- bind_rows(x)
    }
    if(class(x)=='list' && length(x)==1 && is.null(names(x))) x <- x[[1]]
    return(x)
  }
  recf <- function(rec) {
    # Set NULL and empty arrays to NA, and flatten structure
    recfields <- unlist(rec[!names(rec) %in% arrayfield])
    for(subs in arrayfield) {
      rec <- unlist(unname(rec[names(rec)==subs]), recursive=FALSE)
      if(all(sapply(rec, class)=='list' && sapply(rec, length)==1)) rec <- unlist(rec, recursive = FALSE)
    }
    rec <- simple_rapply(rec, flat, inclLists = 'Last')
    arrayfield <- arrayfield[[length(arrayfield)]]
    if(class(rec)!='data.frame') {
      rec <- do.call(data.frame, args=list(I(rec)))
      names(rec) <- arrayfield
    }
    for(field in tagfields) {
      rec[paste0('rec_',field)] <- recfields[[field]]
    }
    if(!is.null(handler)) {
      return(handler(rec))
    } else {
      return(rec)
    }
  }
  arrayfield <- stringr::str_split(arrayfield, '\\.')[[1]]
  repeat {
    page <- mongolite:::mongo_cursor_next_page(cur, pagesize)
    if(length(page)) {
      cnt <- cnt+length(page)
      ret <- bind_rows(lapply(page, recf))
      cache <- bind_rows(cache, ret)
      if(nrow(cache)>cachesize) {
        if(stringsAsFactors) {
          for(col in which(sapply(cache, class)=='character'))
            cache[col] <- as.factor(cache[col])
          result <- plyr::rbind.fill(result, cache) # Slower, but needed to unify factor levels
        } else {
          result <- bind_rows(result, cache)
        }
        cache <- cache[F,]
      }
      if(verbose) cat('\rFound',cnt,'records...   ')
    }
    if(length(page)<pagesize) {
      cat('\n')
      result <- rbind(result, cache)
      break
    }
  }
  return(result)
}

#' Use a function to update documents in a mongo-Db dynamically
#'
#' A regular query to a mongo-db such as used in the update-method sets field statically, or with limited calculations (increment or multiply)
#' But doing more extensive modifications is not possible in this way. If you want, for example to take 2 textfields, concatenate them and store them in the DB,
#' you need to retrieve the document(s), adapt their value and update this value in the database. Potentially, you need a lot of memory to do this in one go, so this function does this sequentially.
#' Its use is comparable to using the apply-family, the following steps are taken:
#' \enumerate{
#'   \item The findqry is executed over the mongo-db, which returns a pointer to the result-list (which is still stored on the server)
#'   \item A page of /emph{pagesize} documents is retrieved, identifier-information is stored
#'   \item The resulting documents are passed on to FUN. Then there are a few possibilites, based on how many fields you want to update, and whether these are arrayfields, or single values:
#'   \itemize{
#'     \item If setfield is a length-one character, and FUN returns a vector, this is interpreted as one value for each document.
#'     \item If setfield is a length-one character, and FUN returns an unnamed list, this is interpreted as an element for each document. Note that these element are coerced to arrays, even if the elements themselves are length-one.
#'     If you want to prevent this (e.g. to mix values and arrays), you can use \code{\link[jsonlite]{unbox}}.
#'     \item If setfield is a character of length>1, FUN is expected to return a list with names equal to the values of setfield. The elements of these lists are treated the same as in the other 2 steps.
#'     \item If setfield is of length 1, it is also allowed to have FUN return a named list of length-one with the name of setfield. This is if you don't know the length of setfield beforehand.
#'   }
#'   \item Updates are done bases on unique values, so if the results are just a few possible values, updating will be faster. For passing on values to mongo-db, \code{\link[jsonlite]{toJSON}}
#'   is used, which influences some details (NA's are converted to null, NULLs to empty arrays, etc.). Use the jsonargs parameter to pass on extra arguments to toJSON.
#' }
#'
#' @param moncol Used when directly calling mongoAdjust, pointer to the mongo-collection. If you use monPlus()$adjust, this is retrieved from monPlus
#' @param findqry Query to find matching documents
#' @param infields Fields to retrieve and pass on to FUN, as a character-vector. Use 'All' (default) to get all possible fields, or a first element of 'Not' to list all fields that should be discarded.
#' @param setfield Field(s) to set, as a character-vector. May be overlapping with infields.
#' @param FUN Function to handle the documents, and give back values to set.
#' \cr input to FUN is a list of retrieved documents (normally of length pagesize, unless the end is reached), and extra arguments passed on with ...
#' \cr If setfield is of length>1, it should give back a named list, with \code{length(FUN())==length(setfield) && names(FUN())==setfield},
#' containing as element unnamed lists or vectors of length equal to the input provided.
#' \cr If setfield is of length one, it can either give back a similar named list of length one, or a similar element (unnamed list or vector of same length as input)
#' @param ... Extra arguments passes on to FUN
#' @param jsonargs List of extra arguments passed on \code{\link[jsonlite]{toJSON}}, see there for details. Useful to specify encoding of dates, NA, NULLs, etc.
#' A number of arguments is arguments has differing defaults: POSIXt and raw defult to 'mongo' if not specified.
#' @param skip Number of document to skip, useful for stopping and resuming later
#' @param limit Limit on number of documents. 0 for unlimited.
#' @param pagesize Number of documents to use for one page. Smaller uses less memory, but is slower.
#' @param verbose Emit extra output (counter after a page has been processed). Takes over the default from a monPlus-object if provided.
#'
#' @return A list with elements modifiedCount and matchedCount, sum of all documents.
#' @examples
#' # Assumed: we can establish a connection to mongodb://localhost:27017,
#' # with documents containing a field firstname and lastname
#' MyMongo <- monPlus('MyCol','MyMon')
#' MyMongo$insert(c('{"OwnID":"Doc1","Author": {"FirstName": "John", "LastName": "Smith"}}',
#' '{"OwnID":"Doc2","Author": {"FirstName": "James", "LastName": "Brown"}}',
#' '{"OwnID":"Doc3","Author": {"FirstName": "George", "LastName": "Watson"}}'))
#' mongoAdjust(MyMongo$col, infields=c('Author.FirstName','Author.LastName'),
#' setfield='Author.FullName',FUN=function(x) {unname(sapply(x, paste, collapse=' '))})
#'
#' # Cleaning up
#' MyMongo$remove('{"OwnID": {"$in": ["Doc1","Doc2","Doc3"]}}')
#' if(MyMongo$count()==0) MyMongo$drop()
#'
#' @importFrom EmilMisc %!in%
#' @export

mongoAdjust <- function(moncol, findqry='{}', infields=c('All'), setfield='extraInfo_from_R', FUN, ..., jsonargs=list(), skip=0, limit=0, pagesize=1000, verbose=FALSE) {
  if(class(moncol)!='mongo_collection' || mongolite:::null_ptr(moncol)) stop('Mongo collection pointer is invalid or dead')
  if(is.null(names(jsonargs)) || 'POSIXt' %!in% names(jsonargs)) jsonargs$POSIXt <- 'mongo'
  if('raw' %!in% names(jsonargs)) jsonargs$raw <- 'mongo'
  if(all(infields=='All')) {
    fields <- '{}'
  } else if(infields[1]=='Not') {
    fields <- infields[infields %!in% c('Not', '_id')]
    fields <- paste0('{"', paste(fields, collapse = '": 0, "'), '": 0}')
  } else {
    fields <- paste0('{"', paste(infields, collapse = '": 1, "'), '": 1}')
    # Doesn't matter if _id is included or not, it defaults to true
  }
  cur <- mongolite:::mongo_collection_find(moncol, query=findqry, fields = fields, skip=skip, limit=limit)
  cnt <- 0
  modCnt <- 0
  matCnt <- 0
  repeat {
    page <- mongolite:::mongo_cursor_next_page(cur, pagesize)
    if(length(page)) {
      # Process page
      if(names(page[[1]])[1]!='_id') stop('\nUnexpected return from mongodb, function mongoAdjust')
      ids <- sapply(page, `[[`,1)
      toFUN <- if(any(c('All','_id') %in% infields)) page else sapply(page, `[`, i=-1)
      setVals <- FUN(toFUN, ...)
      if(length(setfield)==1 && is.null(names(setVals)) && length(setVals)==length(toFUN)) {
        setVals <- list(setVals)
        names(setVals) <- setfield
      }
      if(is.null(names(setVals)) || !identical(names(setVals),setfield) || !all(sapply(setVals, length)==length(toFUN)))
        stop('\nUnexpexted return value from FUN (',modCnt,' modified, out of first ',matCnt,' matched records')
      setVals <- lapply(1:length(toFUN), function(i) {
        lapply(setVals, function(field) {
          if(class(field)=='list') {
            field[[i]]
          } else {
            jsonlite::unbox(field[[i]])
          }
        })
      })
      for(vals in unique(setVals)) {
        idcs <- which(sapply(setVals, identical, y=vals))
        upd <- mongolite:::mongo_collection_update(col=moncol,
              selector = paste0('{"_id": {"$in": [{"$oid":"', paste(ids[idcs], collapse = '"}, {"$oid":"'),'"}]}}'),
              update = paste0('{"$set":',jsonlite::toJSON(vals),'}'),
              filters=NULL, upsert=FALSE,
              multiple = length(idcs)>1, replace=FALSE)
        modCnt <- modCnt+upd$modifiedCount
        matCnt <- matCnt+upd$matchedCount
        if(upd$matchedCount!=length(idcs)) stop('\nError when running update-query, expected ',length(idcs),' to match, instead got ', upd$matchedCount)
      }
    }
    cnt <- cnt+length(page)
    if(verbose) cat('Processed',format(cnt, scientific = FALSE),'records...                              \r')
    if(length(page) < pagesize) {
      break
    }
  }
  cat('Updated',modCnt,'records (out of',matCnt,'found)                                                    \n')
  return(list(modifiedCount=modCnt, matchedCount=matCnt))
}























