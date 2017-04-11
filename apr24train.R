#preparation of data
require(xgboost)
require(DiagrammeR)
train <- read.csv("~/Dropbox/traindata.csv")  #trainning set
y.train <- read.csv('~/Dropbox/trainlabel.csv')  #labels for training set
y.train<-unlist(y.train)
trainMatrix<-as.matrix(train)  #convert to matrix
rm(train)
###model training
param <- list("booster"='gbtree',
              "objective" = "binary:logistic",
              "eval_metric" = "error",
              "eta"=0.05,  #step size
              "max_depth"=20)  #max_depth

cv.nround <-100   # how many trees
cv.nfold <- 5  #fold for CV

# Cross Validation Function to tune the parameters
bst.cv = xgb.cv(param=param, data = trainMatrix, label = y.train, nfold = cv.nfold, nrounds = cv.nround)

# Train the model
bst = xgboost(param=param, data = trainMatrix, label = y.train, nrounds=cv.nround,objective = "binary:logistic")
