library(stringr)
library(dplyr)

df<-read.csv('Urban_Forestry_Street_Trees.csv')
colnames(df)
str(df)
summary(df)
colnames(df)[1]<-'X'
colnames(df)
prc<-df[,c("X","Y","OBJECTID","VICINITY","SCI_NM","CMMN_NM","TREE_NOTES","FAM_NAME","GENUS_NAME")]
prc[is.na(prc[,1]),]
prc<-prc[-c(101268,122376),]
for(i in 1:ncol(prc)){
  if(is.factor(prc[,i]))prc[,i]=as.character(prc[,i])
}
str(prc)
prc[,-c(1,2,3)]<-sapply(prc[,-c(1,2,3)],str_trim)
sapply(prc, function(x) sum(x==''))
temp<-prc
is.na(prc)<-prc==''
sapply(prc, function(x) sum(is.na(x)))
sum(prc$'SCI_NM'=='Other (See Notes)')
temp[temp[,'SCI_NM']=='Other (See Notes)',c(5,6,7,8,9)]%>%sample_n(size=10)
sum(temp[,'FAM_NAME']=='Null')
sum(temp$'CMMN_NM'=='Other (See Notes)')
sum(temp=='Other'|temp=='other')
prc[prc=='Null'|prc=='Other (See Notes)']<-NA
ls<-c('SCI_NM','CMMN_NM','FAM_NAME','GENUS_NAME')
items<-unique(unlist(prc[,ls]))
items[nchar(items)<6]
prc[prc=='X'|prc=='Other'|prc=='No']<-NA
sapply(prc, function(x) sum(is.na(x)))
is.na(prc[is.na(prc$CMMN_NM),ls])[1:10,]
sum(is.na(prc[85,ls]))
prc<-prc[apply(is.na(prc[,ls]),1,sum)<4|grepl(' name',prc$'TREE_NOTES',ignore.case =TRUE),]
sapply(prc, function(x) sum(is.na(x)))
summary(prc)
prc<-prc[apply(is.na(prc[,ls]),1,sum)<1,]
prc<-prc[,-7]
colnames(prc)[1:2]<-c('longitude','latitude')
prc<-sample_n(prc,35000)
write.csv(prc,file='DC_trees.csv',row.names = FALSE)
