import pymongo
from pymongo import MongoClient
import pandas as pd

client = MongoClient()
db = client.test #use the test database which contains collections of coupon training data

##### create feature indicating large/small areas #####
print '1/11 create large small areas now...'
pref = db.prefecture_locations
pref_table = pd.DataFrame(list(pref.find()))
pref_name_help = list(pref_table.columns.values)[-1] #take out the PREF_NAME 
prefecture_list = pref_table[pref_name_help]
combined = db.combined

#take out Tokyo-to,Kanagawa-ken and Osaka-fu(according to wikipedia and our exploratory analysis,these three areas have most population and have most customers&coupons)
large_pref = []
for i in [12,13,26]: #these are their indexes in tables
    large_pref.append(prefecture_list[i])
for i in range(len(large_pref)):
        print large_pref[i] #testing all three large prefectuals

#get location info from combined.csv,both user location and coupon location
location_list = pd.DataFrame(list(combined.find({},{'user_location':1,'_id':0})))
is_large_area = [] #judge if user comes from large prefectuals
for i in range(len(location_list)):
	if location_list['user_location'][i]=='':
		is_large_area.append(None)
	else:
		if location_list['user_location'][i] in large_pref:
			is_large_area.append(1)
		else:
			is_large_area.append(0)

location_list = pd.DataFrame(list(combined.find({},{'coupon_location':1,'_id':0})))

is_large_area2= [] #judge if coupon comes from large prefectuals
for i in range(len(location_list)):
	if location_list['coupon_location'][i]=='':
		is_large_area2.append(None)
	else:
		if location_list['coupon_location'][i] in large_pref:
			is_large_area2.append(1)
		else:
			is_large_area2.append(0)

del location_list

##### transform NA values to 0 #####
print '2/11 transform NA values to 0 now...'

def convertNAtoOne(list):
    newList=[]
    for item in list:
        if item == "NA":  
            newString =int(item.replace("NA", "1"))
            newList.append(newString)
        else:
            newList.append(item)    
    return newList

transform_na = list(combined.find({},{'_id':0,'withdraw_date':1,
    'valid_period':1,
    'usable_mon':1,'usable_tue':1,'usable_wed':1,'usable_thu':1,'usable_fri':1,'usable_sat':1,'usable_sun':1,
    'usable_holiday':1,'usable_date_before_holiday':1}))

transform_na = pd.DataFrame(transform_na)
column = list(transform_na.columns.values)
for c in column:
    transform_na[c] = convertNAtoOne(transform_na[c])

##### transform date to integers #####
print '3/11 transform date to integers now...'

def convertDatetoInt(list):
    newList = []
    for string in list:
        if string != '':
            if string != 'NA':
                newStr=int(string.replace("-", "").replace(":", "").replace(" ", ""))
                newList.append(newStr)
            else:
                newList.append(1)
        else:
            newList.append(None)
    return newList

convert_date = list(combined.find({},{'_id':0,'I_DATE':1,
    'reg_date':1,'bought_date':1,'disp_from':1,
    'disp_end':1,'valid_from':1,'valid_end':1}))
convert_date = pd.DataFrame(convert_date)
column = list(convert_date.columns.values)

for c in column:
    convert_date[c] = convertDatetoInt(convert_date[c])

#### transform location into distances #####
import math
print '4/11 transform location into distances now...'
def calculateDistance(latUser, lonUser, latCoupon, lonCoupon):
    if '' not in [latUser,lonUser,latCoupon,lonCoupon]:
        latUser, lonUser, latCoupon, lonCoupon =map(math.radians, [latUser, lonUser, latCoupon, lonCoupon]) 
        lonDist= lonCoupon-lonUser
        latDist = latCoupon-latUser
        x=math.sin(latDist/2)**2 + math.cos(latUser) * math.cos(latCoupon) * math.sin(lonDist/2)**2
        return 6371*2*math.asin(math.sqrt(x))
    else:
        return None


distance = []
locations = list(combined.find({},{'_id':0,'user_location_la':1,'user_location_lo':1,'coupon_location_la':1,'coupon_location_lo':1}))
for i in range(len(locations)):
    row = locations[i]
    dist = calculateDistance(row['user_location_la'],row['user_location_lo'],row['coupon_location_la'],row['coupon_location_lo'])
    distance.append(dist)
del locations


##### create 4 new fatures #####
##instead of finding coupon information from coupon_list_train table, we'll
##directly look at when customer viewed certain coupon. That will help us
##learn two things: 1. the coupon is still on display on the view date
##2. customer viewing habits as to which day in the week that customer like to
##shop on website.
print '5/11 create 4 new fatures now...'
from datetime import datetime
users = list(db.combined.distinct('USER_ID_hash')) #about 22805 users

## create following 4 features:
# user interested in wkday coupon(visit)
# user interested in wkend coupon(visit)
# user interested in wkday coupon(purchase)
# user interested in wkend coupon(purchase)
visit_wkday_dict = dict.fromkeys(users)
purchase_wkday_dict = dict.fromkeys(users) #initialize dictionary key size can increase performance

records = list(combined.find({},{'_id':0,'I_DATE':1,'USER_ID_hash':1,'PURCHASE_FLG':1}))

print '6/11 build dicts for new feature: wkday now...'

#build dictionaries
for i in range(len(records)):
    date = datetime.strptime(records[i]['I_DATE'],'%Y-%m-%d %H:%M:%S')
    user = records[i]['USER_ID_hash']
    if records[i]['PURCHASE_FLG'] == 0 : # if is visit data
        if visit_wkday_dict[user] == None:
            visit_wkday_dict[user] = {'wkday':0,'wkend':0}
        if date.weekday() <= 5:
            visit_wkday_dict[user]['wkday'] += 1
        else:
            visit_wkday_dict[user]['wkend'] += 1
    else:
        if purchase_wkday_dict[user] == None:
            purchase_wkday_dict[user] = {'wkday':0,'wkend':0}
        if date.weekday() <= 5:
            purchase_wkday_dict[user]['wkday'] += 1
        else:
            purchase_wkday_dict[user]['wkend'] += 1

prefer_wkday_visit = []
prefer_wkday_purchase = []
prefer_wkend_visit = []
prefer_wkend_purchase = []

print '7/11 append new feature list now...'
#use dictionaries to build new feature columns
for i in range(len(records)):
    user = records[i]['USER_ID_hash']
    if visit_wkday_dict[user] == None:
        prefer_wkday_visit.append(None)
        prefer_wkend_visit.append(None)
    else:
        if visit_wkday_dict[user]['wkday'] > visit_wkday_dict[user]['wkend']:
            prefer_wkday_visit.append(1)
            prefer_wkend_visit.append(0)
        elif visit_wkday_dict[user]['wkday'] == visit_wkday_dict[user]['wkend']:
            prefer_wkday_visit.append(1)
            prefer_wkend_visit.append(1)
        else:
            prefer_wkday_visit.append(0)
            prefer_wkend_visit.append(1)

    if purchase_wkday_dict[user] == None:
        prefer_wkday_purchase.append(None)
        prefer_wkend_purchase.append(None)
    else:
        if purchase_wkday_dict[user]['wkday'] > purchase_wkday_dict[user]['wkend']:
            prefer_wkday_purchase.append(1)
            prefer_wkend_purchase.append(0)
        elif purchase_wkday_dict[user]['wkday'] == purchase_wkday_dict[user]['wkend']:
            prefer_wkday_purchase.append(1)
            prefer_wkend_purchase.append(1)
        else:
            prefer_wkday_purchase.append(0)
            prefer_wkend_purchase.append(1)

del visit_wkday_dict,purchase_wkday_dict

## now create two following features:
# top 3 visited categories
# top 3 purchased categories
print '8/11 creating top category features now...'
visit_genre_dict = dict.fromkeys(users)
purchase_genre_dict = dict.fromkeys(users)
genre_list = list(db.combined.distinct('genre'))
empty = [i for i,e in enumerate(genre_list) if e==''][0]
del genre_list[empty]

records = list(combined.find({},{'_id':0,'genre':1,'USER_ID_hash':1,'PURCHASE_FLG':1}))

print '9/11 build dictionaries for category features now...'
for i in range(len(records)):
    user = records[i]['USER_ID_hash']
    genre = records[i]['genre']
    if genre != '':
        if records[i]['PURCHASE_FLG'] == 0 : # if is visit data
            if visit_genre_dict[user] == None: # check if inner library is initialized
                visit_genre_dict[user] = dict.fromkeys(genre_list)
            if visit_genre_dict[user][genre] == None:
                visit_genre_dict[user][genre] = 1
            else:
                visit_genre_dict[user][genre] += 1
        else: # if is purchase data
            if purchase_genre_dict[user] == None: # check if inner library is initialized
                purchase_genre_dict[user] = dict.fromkeys(genre_list)
            if purchase_genre_dict[user][genre] == None:
                purchase_genre_dict[user][genre] = 1
            else:
                purchase_genre_dict[user][genre] += 1

def top3genre(genre_dict,top=3):
    i = 0
    result = []
    if genre_dict != None:
        while i <=top:
            max_num = max(genre_dict.values())
            if max_num != None:
                index = [a for a,e in enumerate(genre_dict.values()) if e == max_num][0]
                key_c = genre_dict.keys()[index]
                result.append(key_c)
                genre_dict = {key: value for (key, value) in genre_dict.items() if key != key_c} #deleting the key with max value
                i += 1
            else:
                return result
        return result
    else:
        return result

top_genre_visit = []
top_genre_purchase = []

print '10/11 append feature list for categories now...'

for i in range(len(records)):
    user = records[i]['USER_ID_hash']
    genre = records[i]['genre']
    if genre != '':
        top_visit = top3genre(visit_genre_dict[user])
        top_purchase = top3genre(purchase_genre_dict[user])
        if genre in top_visit:
            top_genre_visit.append(1)
        else:
            top_genre_visit.append(0)
        if genre in top_purchase:
            top_genre_purchase.append(1)
        else:
            top_genre_purchase.append(0)
    else:
        top_genre_visit.append(None)
        top_genre_purchase.append(None)


##### combine transfomred data and created new features into result #####
print '11/11 final combine process now...'

result = pd.DataFrame(list(combined.find({},{'_id':0,'I_DATE':0,
    'reg_date':0,'bought_date':0,'disp_from':0,
    'disp_end':0,'valid_from':0,'valid_end':0,
    'withdraw_date':0,'valid_period':0,
    'usable_mon':0,'usable_tue':0,'usable_wed':0,'usable_thu':0,'usable_fri':0,'usable_sat':0,'usable_sun':0,
    'usable_holiday':0,'usable_date_before_holiday':0,
    'coupon_location':0,'user_location':0,
    'genre':0,'large_area_name':0,
    'user_location_la':0,'user_location_lo':0,'coupon_location_la':0,'coupon_location_lo':0})))

print 'add column:transform_na'
result = pd.concat([result.reset_index(drop=True),transform_na],axis=1)
print 'add column:convert_date'
result = pd.concat([result.reset_index(drop=True),convert_date],axis=1)

print 'add column:large_area'
result['user_large_area'] = is_large_area
result['coupon_large_area'] = is_large_area2
print 'add column:distance'
result['distance'] = distance

print 'add column:prefer_wkday_visit/purchase'
result['prefer_wkday_visit'] = prefer_wkday_visit
result['prefer_wkend_visit'] = prefer_wkend_visit
result['prefer_wkday_purchase'] = prefer_wkday_purchase
result['prefer_wkend_purchase'] = prefer_wkend_purchase
print 'add column:top_genre_visit/purchase'
result['top_genre_visit'] = top_genre_visit
result['top_genre_purchase'] = top_genre_purchase
print 'writing table:transformed.csv'
result.to_csv('transformed_new.csv',encoding='utf-8',index=False)
