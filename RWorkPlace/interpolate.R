path <- "file:///F:/myWorkplace/data/Bankscope插值数据.csv"

data <- read.csv(path, header = T, sep = ",", encoding = "UTF-8")

for(i in dim(data)[2]) {
  data[, i] <- as.numeric(data[, i])
}
data[1, ]
as.numeric(t(data))




x <- 1 : 10
y1 <- as.vector(data[2, ][2 : 11])
y2 <- as.vector(data[3, ][2 : 11])
plot(x, y1)
lines(x, y1)
lines(x, y2)
library(kernlab)

data(spam)

## create test and training set
index <- sample(1:dim(spam)[1])
spamtrain <- spam[index[1:floor(dim(spam)[1]/2)], ]
spamtest <- spam[index[((ceiling(dim(spam)[1]/2)) + 1):dim(spam)[1]], ]


filter <- ksvm(type~.,data=spamtrain,kernel="rbfdot",
               kpar=list(sigma=0.05),C=5,cross=3)
filter
