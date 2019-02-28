library(TSA)
data(arma11.s)

z <- arma11.s


ar.max = 7
ma.max = 13

lag1 <- function(z, lag = 1) {
  c(rep(NA, lag), z[1:(length(z) - lag)])
}
reupm <- function(m1, nrow, ncol) {
  k <- ncol - 1
  m2 <- NULL
  for (i in 1:k) {
    i1 <- i + 1
    work <- lag1(m1[, i])
    work[1] <- -1
    temp <- m1[, i1] - work * m1[i1, i1]/m1[i, i]
    temp[i1] <- 0
    m2 <- cbind(m2, temp)
  }
  m2
}
ceascf <- function(m, cov1, nar, ncol, count, ncov, z, zm) {
  result <- 0 * seq(1, nar + 1)
  result[1] <- cov1[ncov + count]
  for (i in 1:nar) {
    temp <- cbind(z[-(1:i)], zm[-(1:i), 1:i]) %*% c(1, 
                                                    -m[1:i, i])
    result[i + 1] <- acf(temp, plot = FALSE, lag.max = count, 
                         drop.lag.0 = FALSE)$acf[count + 1]
  }
  result
}
ar.max <- ar.max + 1
ma.max <- ma.max + 1
nar <- ar.max - 1
nma <- ma.max
ncov <- nar + nma + 2
nrow <- nar + nma + 1
ncol <- nrow - 1
z <- z - mean(z)
zm <- NULL
for (i in 1:nar) zm <- cbind(zm, lag1(z, lag = i))
cov1 <- acf(z, lag.max = ncov, plot = FALSE, drop.lag.0 = FALSE)$acf
cov1 <- c(rev(cov1[-1]), cov1)
ncov <- ncov + 1
m1 <- matrix(0, ncol = ncol, nrow = nrow)
for (i in 1:ncol) m1[1:i, i] <- ar.ols(z, order.max = i, 
                                       aic = FALSE, demean = FALSE, intercept = FALSE)$ar
eacfm <- NULL
for (i in 1:nma) {
  m2 <- reupm(m1 = m1, nrow = nrow, ncol = ncol)
  ncol <- ncol - 1
  eacfm <- cbind(eacfm, ceascf(m2, cov1, nar, ncol, i, 
                               ncov, z, zm))
  m1 <- m2
}
work <- 1:(nar + 1)
work <- length(z) - work + 1
symbol <- NULL
for (i in 1:nma) {
  work <- work - 1
  symbol <- cbind(symbol, ifelse(abs(eacfm[, i]) > 2/work^0.5, 
                                 "x", "o"))
}
rownames(symbol) <- 0:(ar.max - 1)
colnames(symbol) <- 0:(ma.max - 1)
cat("AR/MA\n")
print(symbol, quote = FALSE)
invisible(list(eacf = eacfm, ar.max = ar.max, ma.ma = ma.max, 
               symbol = symbol))


eacf(arma11.s)
