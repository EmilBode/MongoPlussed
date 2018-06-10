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
#' # Assumed: we can establish a connection to mongodb://localhost:27017, and that this is a new or empty database/collection
#' MyMongo <- monPlus('MyCol','MyMon')
#' MyMongo$insert(c('{"OwnID":"Doc1","Authors": {"Name": ["John Smith", "George"], "RegistrNo": [1,2]}}',
#' '{"OwnID":"Doc2","Authors": {"Name": ["John Smith", "William Smith"], "RegistrNo": [3,4]}}',
#' '{"OwnID":"Doc3","Authors": {"Name": ["William", "George Smith"], "RegistrNo": [4,2]}}'))
#' taggedMongofind(MyMongo$col, '{"Authors.Name": "John Smith"}',arrayfield = 'Authors', tagfields = 'OwnID')
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
    return(x)
  }
  recf <- function(rec) {
    # Set NULL and empty arrays to NA, and flatten structure
    recfields <- unlist(rec[!names(rec) %in% arrayfield])
    for(subs in arrayfield[-length(arrayfield)]) {
      rec <- unlist(unname(rec[names(rec)==subs]), recursive=FALSE)
      if(all(sapply(rec, class)=='list' && sapply(rec, length)==1)) rec <- unlist(rec, recursive = FALSE)
    }
    rec <- simple_rapply(rec, flat, inclLists = 'Last')
    for(field in tagfields) {
      rec[[arrayfield]][paste0('rec_',field)] <- rec[field]
    }
    if(!is.null(handler)) {
      return(handler(rec[[arrayfield]]))
    } else {
      return(rec[[arrayfield]])
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




