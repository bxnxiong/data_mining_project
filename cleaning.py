import pymongo
from pymongo import MongoClient
import pandas as pd

##### environment setup #####

client = MongoClient()
#database names: coupon_list_train
db = client.test #use the test database which contains collections of coupon training data


##### import collections #####

#coupon_area = db.coupon_area_train #count:138185 but we're not using it because coupon_visit
#already has all info in this table
coupon_detail = db.coupon_detail_train #count:168996
coupon_list = db.coupon_list_train #count:19413
coupon_visit = db.coupon_visit_train #count:2833180
pref = db.prefecture_locations #count:47
user_list = db.user_list #count:22873


##### create helper variables #####

## prefecture_location columns to be combined into coupon_visit table
pref_table = pd.DataFrame(list(pref.find()))
pref_name_help = list(pref_table.columns.values)[-1] #take out the PREF_NAME 
#column name since it is encoded not as 'PREF_NAME' but as '\ufeffPREF_NAME',so is difficult to
#query directly with 'PREF_NAME'

## get coupon ids, customer ids and purchase ids from coupon_visit
ids = list(coupon_visit.find({},{"_id":0,"VIEW_COUPON_ID_hash":1,"USER_ID_hash":1,"PURCHASEID_hash":1}))
#ids[0] = {u'VIEW_COUPON_ID_hash': u'34c48f84026e08355dc3bd19b427f09a', 
#u'USER_ID_hash': u'd9dca3cb44bab12ba313eaa681f663eb'};data type:dictionary
n = len(ids)


##### create empty lists to store query results to be combined later #####

## coupon_detail columns to be combined into coupon_visit table
amount = [] #bought coupon count; from coupon_detail.ITEM_COUNT
bought_date = [] #purchase date; from coupon_detail.I_DATE

## user_list columns to be combined into coupon_visit table
reg_date = [] #from user_list.REG_DATE
gender = [] #from user_list.SEX_ID
age = [] #from user_list.AGE
withdraw_date = [] #from user_list.WITHDRAW_DATE
user_location = [] #buyer residential place; from user_list.PREF_NAME

## coupon_list columns to be combined into coupon_visit table
capsule_text = []
genre = []
price_rate = []
catalog_price = []
discount_price = []
disp_from = []
disp_end = []
disp_period = []
valid_from = []
valid_end = []
valid_period = []
usable_mon = []
usable_tue = []
usable_wed = []
usable_thu = []
usable_fri = []
usable_sat = []
usable_sun = []
usable_holiday = []
usable_date_before_holiday = []
large_area_name = []
coupon_location = [] #equivalent to PREF_NAME in other tables


##### indexing to speed up query #####
## indexing frequent query field will massively increase the speed!!!

coupon_detail.create_index([('PURCHASEID_hash',pymongo.ASCENDING)]) 
coupon_list.create_index([('COUPON_ID_hash',pymongo.ASCENDING)])
user_list.create_index([('USER_ID_hash',pymongo.ASCENDING)])

##### start to match and combine #####

for i in range(0,n):
    coupon = ids[i]['VIEW_COUPON_ID_hash']
    user = ids[i]['USER_ID_hash']
    purchase = ids[i]['PURCHASEID_hash']
    print i
    
    #match records from coupon_list
    if coupon_list.find({'COUPON_ID_hash':coupon}).count()>0:
        matching_record = coupon_list.find({'COUPON_ID_hash':coupon})[0]
        capsule_text.append(matching_record['CAPSULE_TEXT'])
        genre.append(matching_record['GENRE_NAME'])
        price_rate.append(matching_record['PRICE_RATE'])
        catalog_price.append(matching_record['CATALOG_PRICE'])
        discount_price.append(matching_record['DISCOUNT_PRICE'])
        disp_from.append(matching_record['DISPFROM'])
        disp_end.append(matching_record['DISPEND'])
        disp_period.append(matching_record['DISPPERIOD'])
        valid_from.append(matching_record['VALIDFROM'])
        valid_end.append(matching_record['VALIDEND'])
        valid_period.append(matching_record['VALIDPERIOD'])
        usable_mon.append(matching_record['USABLE_DATE_MON'])
        usable_tue.append(matching_record['USABLE_DATE_TUE'])
        usable_wed.append(matching_record['USABLE_DATE_WED'])
        usable_thu.append(matching_record['USABLE_DATE_THU'])
        usable_fri.append(matching_record['USABLE_DATE_FRI'])
        usable_sat.append(matching_record['USABLE_DATE_SAT'])
        usable_sun.append(matching_record['USABLE_DATE_SUN'])
        usable_holiday.append(matching_record['USABLE_DATE_HOLIDAY'])
        usable_date_before_holiday.append(matching_record['USABLE_DATE_BEFORE_HOLIDAY'])
        large_area_name.append(matching_record['large_area_name'])
        coupon_location.append(matching_record['ken_name'])
    else:
        capsule_text.append("")
        genre.append("")
        price_rate.append("")
        catalog_price.append("")
        discount_price.append("")
        disp_from.append("")
        disp_end.append("")
        disp_period.append("")
        valid_from.append("")
        valid_end.append("")
        valid_period.append("")
        usable_mon.append("")
        usable_tue.append("")
        usable_wed.append("")
        usable_thu.append("")
        usable_fri.append("")
        usable_sat.append("")
        usable_sun.append("")
        usable_holiday.append("")
        usable_date_before_holiday.append("")
        large_area_name.append("")
        coupon_location.append("")
        
    #match records from coupon_detail
    if coupon_detail.find({'PURCHASEID_hash':purchase}).count()>0:
        matching_record = coupon_detail.find({'PURCHASEID_hash':purchase},{'_id':0})[0]
        if(purchase!=""):
            amount.append(matching_record['ITEM_COUNT']) #bought coupon count; from coupon_detail.ITEM_COUNT
            bought_date.append(matching_record['I_DATE']) #purchase date; from coupon_detail.I_DATE
        else:
            amount.append('')
            bought_date.append('')
    else:
        amount.append('')
        bought_date.append('')
        
    #match records from user_list
    if user_list.find({'USER_ID_hash':user},{'_id':0}).count()>0:
        matching_record = user_list.find({'USER_ID_hash':user},{'_id':0})[0]
        reg_date.append(matching_record['REG_DATE'])
        gender.append(matching_record['SEX_ID'])
        age.append(matching_record['AGE'])
        withdraw_date.append(matching_record['WITHDRAW_DATE'])
        user_location.append(matching_record['PREF_NAME'])
    else:
        reg_date.append('')
        gender.append('')
        age.append('')
        withdraw_date.append('')
        user_location.append('')
       
## end for loop


##### add columns to final table and remove variables #####
## except user_location and coupon_location

result = pd.DataFrame(list(coupon_visit.find()))
## columns from user_list
result['reg_date']=reg_date
result['gender']=gender
result['age']=age
result['withdraw_date']=withdraw_date
result['user_location']=user_location

## columns from coupon_detail
result['amount']=amount
result['bought_date']=bought_date

## columns from coupon_list
result['capsule_text']=capsule_text
result['genre']=genre
result['price_rate']=price_rate
result['catalog_price']=catalog_price
result['discount_price']=discount_price
result['disp_from']=disp_from
result['disp_end']=disp_end
result['disp_period']=disp_period
result['valid_from']=valid_from
result['valid_end']=valid_end
result['valid_period']=valid_period
result['usable_mon']=usable_mon
result['usable_tue']=usable_tue
result['usable_wed']=usable_wed
result['usable_thu']=usable_thu
result['usable_fri']=usable_fri
result['usable_sat']=usable_sat
result['usable_sun']=usable_sun
result['usable_holiday']=usable_holiday
result['usable_date_before_holiday']=usable_date_before_holiday
result['large_area_name']=large_area_name
result['coupon_location']=coupon_location

## delete variables
# del amount,bought_date,reg_date,gender,age,withdraw_date,capsule_text,genre,price_rate
# del catalog_price,discount_price,disp_from,disp_end,disp_period,valid_from,valid_end,valid_period
# del usable_holiday,usable_sun,usable_sat,usable_fri,usable_thu,usable_wed,usable_tue,usable_mon
# del usable_date_before_holiday,large_area_name


##### add latitude and longitude information ######
## for user_location and coupon_location variables

user_location_la = []
user_location_lo = []
coupon_location_la = []
coupon_location_lo = []

pref.create_index([(pref_name_help,pymongo.ASCENDING)])

for i in range(0,n):
    print i
    ## match user_location locations
    if pref.find({pref_name_help:user_location[i]}).count()>0:
        matching_record = pref.find({pref_name_help:user_location[i]})[0]
        user_location_la.append(matching_record['LATITUDE'])
        user_location_lo.append(matching_record['LONGITUDE'])
    else:
        user_location_la.append(None)
        user_location_lo.append(None)
    ## match coupon_location locations
    if pref.find({pref_name_help:coupon_location[i]}).count()>0:
        matching_record = pref.find({pref_name_help:coupon_location[i]})[0]
        coupon_location_la.append(matching_record['LATITUDE'])
        coupon_location_lo.append(matching_record['LONGITUDE'])
    else:
        coupon_location_la.append(None)
        coupon_location_lo.append(None)
       
## create columns for locations
result['user_location_la']=user_location_la
result['user_location_lo']=user_location_lo
result['coupon_location_la']=coupon_location_la
result['coupon_location_lo']=coupon_location_lo

## delete variables
#del user_location_lo,user_location_la,coupon_location_lo,coupon_location_la

##### output final table into csv file "combined.csv" #####
result.to_csv('combined.csv',encoding='utf-8')
