# 时序数据预测 --时序回归所可能用到的拟合模型或回归模型

# 要分析的数据 data.frame
data <- mutate(
  read.csv("file:///E:/上海出差/时序数据.csv"),
  "timestamp" = 
    (as.numeric(as.POSIXct(time, format="%Y/%m/%d")) - 696787200) / 2419200)

# 标签列 dist
# 特征列 speed
set.seed(1123)
num <- dim(data)[1]
train_num <- floor(floor(num * 0.8) / 2) * 2
develop_num <- floor((num - train_num) / 2)

train_id <- sort(sample(num, train_num, F))
train_data <- data[train_id, ]
develop_id <- sort(sample(num - train_num, develop_num, F))
develop_data <- data[-train_id, ][develop_id, ]
test_data <- data[-train_id, ][-develop_id, ]

# 评价模型性能 --均方误差
mse <- function(dt, fit){
  notNaFit <- fit[!is.na(fit)]
  correspondDt <- dt[!is.na(fit)]
  fit_num <- length(notNaFit)
  train_loess = 0.0
  for (i in 1 : fit_num) {
    train_loess = train_loess + (notNaFit[i] - correspondDt[i]) ^ 2 / fit_num
  }
  return(train_loess)
}


# ----
# loess -- stats::loess
# ----
# 训练模型 span是拟合度
loess_model <- loess(labels ~ time, data = train_data, span = 0.4)

train_loess = mse(train_data[, "labels"], loess_model$fitted)
develop_loess = mse(develop_data[, "labels"], predict(loess_model, develop_data[, "timestamp"]))
# 参数调试
span_grids <- seq(0.3, 10, 0.1)
best_model = 0
mini_loss = + Inf
best_param = 0
for(span in span_grids) {
  tmp_model <- loess(labels ~ 1 + time, data = train_data, span)
  develop_loess = mse(develop_data[, "labels"], predict(tmp_model, develop_data[, "timestamp"]))
  if(develop_loess < mini_loss){
    best_model <- tmp_model
    mini_loss <- develop_loess
    best_param <- span
  }
}
print(best_param)
print(mini_loss)
# 可视化
plot(train_data)
lines(train_data$time, best_model$fitted, col = "red", lty=2, lwd=2)


# ----
# 平滑样条插值
# ----
spline_model <- smooth.spline(train_data[,"timestamp"], train_data[,"labels"], df = 2)
mse(train_data[, "labels"], spline_model$y)

findBestModel <- function(train_data, develop_data, span_grids){
  best_model = 0
  mini_loss = + Inf
  best_param = 0
  for(span in span_grids) {
    tmp_model <- loess(labels ~ 1 + timestamp, data = train_data, span = span)
    develop_loess = mse(develop_data[, "labels"], predict(tmp_model, develop_data[, "timestamp"]))
    if(develop_loess < mini_loss){
      best_model <- tmp_model
      mini_loss <- develop_loess
      best_param <- span
    }
  }
  print(best_param)
  print(mini_loss)
  return(best_model)
}

span_grids <- seq(2, 10, 1)
bestModel <- findBestModel(train_data, develop_data, span_grids)
mse(train_data[, "labels"], bestModel$y)

# 效果可视化
plot(train_data[, c("timestamp", "labels")], col = "blue", type = "p", lwd = 2, main = "样条插值")
lines(bestModel$x, bestModel$y, col = "red")
data.frame(train_data, "predict" = bestModel$y)





require(graphics)
plot(dist ~ speed, data = cars, main = "data(cars)  &  smoothing splines")
cars.spl <- with(cars, smooth.spline(speed, dist))
cars.spl
## This example has duplicate points, so avoid cv = TRUE

lines(cars.spl, col = "blue")
ss10 <- smooth.spline(cars[,"speed"], cars[,"dist"], df = 10)
lines(ss10, lty = 2, col = "red")
legend(5,120,c(paste("default [C.V.] => df =",round(cars.spl$df,1)),
               "s( * , df = 10)"), col = c("blue","red"), lty = 1:2,
       bg = 'bisque')
