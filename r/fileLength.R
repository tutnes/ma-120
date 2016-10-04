fileLength <- function(fileNameurl) {
  x = read.table(fileNameurl)
  len = length(x[,1])
  return (len)
}