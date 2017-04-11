#load in the test data
test <- read.csv("~/Dropbox/testdata.csv")  #test set
y.test <- read.csv('~/Dropbox/testlabel.csv')  #labels for test set
y.test<-unlist(y.test)
testMatrix<-as.matrix(test)  #convert to matrix
rm(test)
# Output the result as probability
preds = predict(bst, testMatrix)  ##prediction with probability only

##input the user id and coupon id to create a new table
idtable <- read.csv("~/Dropbox/idtable.csv", stringsAsFactors = FALSE )  #col1=userhash, col2=couponhash
idtable[,3]=preds  #probability is now the third col
colnames(idtable)[3]="prob"
#load in the sample submission
submit <- read.csv("~/Dropbox/sample_submission.csv", stringsAsFactors = FALSE )  #col1=userhash, col2=couponhash
submit[,2:11]=NA
colnames(submit)[2:11]=c("c1","c2","c3","c4","c5","c6","c7","c8","c9","c10")
coupons=c()
for(i in 1:dim(submit)[1]){  # for each users in the submit table
  if(submit[i,1] %in% idtable[,1]){  # if the user is in idtable as well
    temp=subset(idtable, USER_ID_hash==submit[i,1], select=c(VIEW_COUPON_ID_hash,prob))
    #select his relevant information
    sorting=temp[order(temp[,2], decreasing=TRUE),] 
    #sort the coupon he views by probability
    len=dim(sorting)[1]  #how many coupons relevant to the user
    if(len>=10){  #if number of coupons >=10, take top 10
      coupons=sorting[1:10,1]
      submit[i,2:11]=coupons
    }
    else{  #else take all
      coupons=sorting[1:len,1]
      submit[i,2:(len+1)]=coupons
    }
  }
}

write.csv(submit, file = "submit.csv")  #finally write out the submission file