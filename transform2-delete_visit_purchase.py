import pymongo
from pymongo import MongoClient
import pandas as pd

client = MongoClient()
db = client.test #use the test database which contains collections of coupon training data

data = db.transformed_new
gender = list(data.find({},{'_id':0,'gender':1}))
is_male = []
for i in gender:
	if i == 'm':
		is_male.append(1)
	else:
		is_male.append(0)
x = pd.DataFrame(list(data.find({},{'_id':0,
    'capsule_text':0,'gender':0,
    'PAGE_SERIAL':0,'amount':0,'I_DATE':0,'bought_date':0})))
x['is_male']=is_male
x.to_csv('transformed_new2.csv',encoding='utf-8',index=False)
