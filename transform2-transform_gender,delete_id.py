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
#y = list(data.find({},{'_id':0,'PURCHASE_FLG':1}))
x = pd.DataFrame(list(data.find({},{'_id':0,'PURCHASEID_hash':0,
    'PURCHASE_FLG':0,
    'REFERRER_hash':0,'SESSION_ID_hash':0,'USER_ID_hash':0,'VIEW_COUPON_ID_hash':0,
    'capsule_text':0,'gender':0,
    'PAGE_SERIAL':0,'amount':0,'I_DATE':0,'bought_date':0})))
x['is_male']=is_male
x.to_csv('data_x.csv',encoding='utf-8',index=False)
#y = pd.DataFrame(y)
#y.to_csv('data_y.csv',encoding='utf-8',index=False)