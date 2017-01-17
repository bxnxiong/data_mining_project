import pymongo
from pymongo import MongoClient
import pandas as pd
import numpy as np

print '##### environment setup #####'

client = MongoClient()
db = client.test 

print '##### import collections #####'

#test user ids
user_test = list(db.sample_submission.find({},{'_id':0,'USER_ID_hash':1}))
users = []
for i in range(len(user_test)):
	users.append(user_test[i]['USER_ID_hash'])

del user_test
#coupon_area = db.coupon_area_test #again we're not using it because coupon_list_test
#already has all info in this table. Even if we use coupon_area_test data, in training data
#coupon_area_train has distinct coupon_ids #19368 while coupon_list_train has distinct #19413
#what's more,coupon_list table has more information about coupon so we use coupon_list as base
#table for coupon info
#also we checked that in coupon_list_test there are no duplicate area information for same coupon
#id, so we'll use the area information about test coupon in coupon_list_test as our input,
#instead of from coupon_area table
coupon_list = list(db.coupon_list_test.find()) #count:19413
pref = db.prefecture_locations #count:47
pref_table = pd.DataFrame(list(pref.find()))
pref_name_help = list(pref_table.columns.values)[-1] #take out the PREF_NAME 
prefecture_list = pref_table[pref_name_help]
combined = db.combined

print '##### define large_pref #####'
#take out Tokyo-to,Kanagawa-ken and Osaka-fu(according to wikipedia and our exploratory analysis,these three areas have most population and have most customers&coupons)
large_pref = []
for i in [12,13,26]: #these are their indexes in tables
    large_pref.append(prefecture_list[i])
for i in range(len(large_pref)):
        print large_pref[i] #testing all three large prefectuals

data = db.transformed_new2

print '##### get user and coupon locations #####'

user_location_la = []
user_location_lo = []

coupon_location = []
coupon_location_la = []
coupon_location_lo = []

db.combined.create_index([('USER_ID_hash',pymongo.ASCENDING)]) 
print len(users)
for i in range(len(users)):
	if i%1000 == 0:
		print i
	match_record = list(db.user_list.find({'USER_ID_hash':users[i]}))
	if len(match_record)>0:
		user_location=match_record[0]['PREF_NAME']
		if user_location != '':
			matching_record = list(pref.find({pref_name_help:user_location}))[0]
			user_location_la.append(matching_record['LATITUDE'])
			user_location_lo.append(matching_record['LONGITUDE'])
		else:
			user_location_la.append('')
			user_location_lo.append('')
	else:	#in our data we have user id=u'0596477fcb70cc48e4ffae2e3c25143d' who only registered but
	#not visit or buy any coupon
		user_location_la.append('')
		user_location_lo.append('')

print '##### get coupon_large_area and distance btw user and coupon #####'

for i in range(len(coupon_list)):
	coupon_location.append(coupon_list[i]['ken_name'])
	if len(list(pref.find({pref_name_help:coupon_location[i]}))) > 0:
		matching_record = pref.find({pref_name_help:coupon_location[i]})[0]
		coupon_location_la.append(matching_record['LATITUDE'])
		coupon_location_lo.append(matching_record['LONGITUDE'])
	else:
		coupon_location_la.append('')
		coupon_location_lo.append('')



distance = []
import math
print '# transform location into distances now...'
def calculateDistance(latUser, lonUser, latCoupon, lonCoupon):
    if '' not in [latUser,lonUser,latCoupon,lonCoupon]:
        latUser, lonUser, latCoupon, lonCoupon =map(math.radians, [latUser, lonUser, latCoupon, lonCoupon]) 
        lonDist= lonCoupon-lonUser
        latDist = latCoupon-latUser
        x=math.sin(latDist/2)**2 + math.cos(latUser) * math.cos(latCoupon) * math.sin(lonDist/2)**2
        return 6371*2*math.asin(math.sqrt(x))
    else:
        return ''


for i in range(len(user_location_la)):
	for j in range(len(coupon_location_la)):
		# print 'user_location_la[i],user_location_lo[i]',user_location_la[i],user_location_lo[i]
		# print 'coupon_location_la[j],coupon_location_lo[j]',coupon_location_la[j],coupon_location_lo[j]
		dist = calculateDistance(user_location_la[i],user_location_lo[i],coupon_location_la[j],coupon_location_lo[j])
		distance.append(dist)

del user_location_la,user_location_lo,coupon_location_la,coupon_location_lo

coupon_large_area = [] #judge if coupon comes from large prefectuals
for i in range(len(coupon_location)):
	if coupon_location[i]=='':
		coupon_large_area.append('')
	else:
		if coupon_location[i] in large_pref:
			coupon_large_area.append(1)
		else:
			coupon_large_area.append(0)


del coupon_location

print '##### build genre preference #####'
visit_genre_dict = dict.fromkeys(users)
purchase_genre_dict = dict.fromkeys(users)
genre_list = list(db.combined.distinct('genre'))
empty = [i for i,e in enumerate(genre_list) if e==''][0]
del genre_list[empty]

records = list(combined.find({},{'_id':0,'genre':1,'USER_ID_hash':1,'PURCHASE_FLG':1}))

print '#build dictionaries for category features now...'

# for i in range(len(records)):
#     user = records[i]['USER_ID_hash']
#     genre = records[i]['genre']
#     if genre != '':
#         if records[i]['PURCHASE_FLG'] == 0 : # if is visit data
#             if visit_genre_dict[user] == None: # check if inner library is initialized
#                 visit_genre_dict[user] = dict.fromkeys(genre_list)
#             if visit_genre_dict[user][genre] == None:
#                 visit_genre_dict[user][genre] = 1
#             else:
#                 visit_genre_dict[user][genre] += 1
#         else: # if is purchase data
#             if purchase_genre_dict[user] == None: # check if inner library is initialized
#                 purchase_genre_dict[user] = dict.fromkeys(genre_list)
#             if purchase_genre_dict[user][genre] == None:
#                 purchase_genre_dict[user][genre] = 1
#             else:
#                 purchase_genre_dict[user][genre] += 1


for i in range(len(records)):
	user = records[i]['USER_ID_hash']
	genre = records[i]['genre']
	if genre != '':
		if records[i]['PURCHASE_FLG'] == 0:
			if visit_genre_dict.has_key(user):
				if visit_genre_dict[user] == None:
					visit_genre_dict[user] = dict.fromkeys(genre_list)
				if visit_genre_dict[user][genre] == None:
					visit_genre_dict[user][genre] = 1
				else:
					visit_genre_dict[user][genre] += 1
		else:
			if purchase_genre_dict.has_key(user):
				if purchase_genre_dict[user] == None:
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
                genre_dict = {key: value for (key, value) in genre_dict.items() if key != key_c}
                i += 1
            else:
                return result
        return result
    else:
        return result

print '#append feature list for categories now...'

top_genre_visit = []
top_genre_purchase = []


coupon_genre=[]
for i in range(len(coupon_list)):
	coupon_genre.append(coupon_list[i]['GENRE_NAME'])

for i in range(len(users)):
	if i%1000 == 0:
		print i
	for j in range(len(coupon_genre)):
	    user = users[i]
	    genre = coupon_genre[j]
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

print '##### gathering user and coupon data into DataFrame now... #####'

user_id = []
age = []
is_male = []
user_large_area = []
prefer_wkday_visit = []
prefer_wkend_visit = []
prefer_wkday_purchase = []
prefer_wkend_purchase = []
reg_date = []
withdraw_date = []


coupon_id = []
catelog_price = []
discount_price = []
disp_end = []
disp_from = []
disp_period = []
price_rate = []
usable_date_before_holiday = []
usable_fri = []
usable_holiday = []
usable_mon = []
usable_sat = []
usable_sun = []
usable_thu = []
usable_tue = []
usable_wed = []
valid_end = []
valid_from = []
valid_period = []
#coupon_location = []

coupon_large_area2 = []
distance2 = []

data.create_index([('USER_ID_hash',pymongo.ASCENDING)]) 

for i in range(len(users)):
	if i%1000 == 0:
		print i
	for j in range(len(coupon_list)):
		if j%10 == 0:
			print 'i,j',i,j
		user_id.append(users[i])
		
		if len(list(data.find({'USER_ID_hash':users[i]}))) > 0:
			match_data = data.find_one({'USER_ID_hash':users[i]})
			age.append(match_data['age'])
			is_male.append(match_data['is_male'])
			user_large_area.append(match_data['user_large_area'])
			prefer_wkday_visit.append(match_data['prefer_wkday_visit'])
			prefer_wkend_visit.append(match_data['prefer_wkend_visit'])
			prefer_wkday_purchase.append(match_data['prefer_wkday_purchase'])
			prefer_wkend_purchase.append(match_data['prefer_wkend_purchase'])
			reg_date.append(match_data['reg_date'])
			withdraw_date.append(match_data['withdraw_date'])
		else:
			age.append('')
			is_male.append('')
			user_large_area.append('')
			prefer_wkday_visit.append('')
			prefer_wkend_visit.append('')
			prefer_wkday_purchase.append('')
			prefer_wkend_purchase.append('')
			reg_date.append('')
			withdraw_date.append('')

for i in range(len(users)):
	if i%1000 == 0:
		print i
	for j in range(len(coupon_list)):
		coupon_id.append(coupon_list[j]['COUPON_ID_hash'])
		catelog_price.append(coupon_list[j]['CATALOG_PRICE'])
		discount_price.append(coupon_list[j]['DISCOUNT_PRICE'])
		disp_end.append(coupon_list[j]['DISPEND'])
		disp_from.append(coupon_list[j]['DISPFROM'])
		disp_period.append(coupon_list[j]['DISPPERIOD'])
		price_rate.append(coupon_list[j]['PRICE_RATE'])
		usable_date_before_holiday.append(coupon_list[j]['USABLE_DATE_BEFORE_HOLIDAY'])
		usable_fri.append(coupon_list[j]['USABLE_DATE_FRI'])
		usable_holiday.append(coupon_list[j]['USABLE_DATE_HOLIDAY'])
		usable_mon.append(coupon_list[j]['USABLE_DATE_MON'])
		usable_sat.append(coupon_list[j]['USABLE_DATE_SAT'])
		usable_sun.append(coupon_list[j]['USABLE_DATE_SUN'])
		usable_thu.append(coupon_list[j]['USABLE_DATE_THU'])
		usable_tue.append(coupon_list[j]['USABLE_DATE_TUE'])
		usable_wed.append(coupon_list[j]['USABLE_DATE_WED'])
		valid_end.append(coupon_list[j]['VALIDEND'])
		valid_from.append(coupon_list[j]['VALIDFROM'])
		valid_period.append(coupon_list[j]['VALIDPERIOD'])
		coupon_large_area2.append(coupon_large_area[j])
		distance2.append(distance[j])


print '##### writing lists to DataFrame now... #####'

df = pd.DataFrame() #table with user_id and coupon_id
df['user_id']=user_id
df['coupon_id']=coupon_id
df['age']=age
df['catelog_price']=catelog_price
df['coupon_large_area']=coupon_large_area2
df['discount_price']=discount_price
df['disp_end']=disp_end
df['disp_from']=disp_from
df['disp_period']=disp_period
df['distance']=distance2
df['prefer_wkday_purchase']=prefer_wkday_purchase
df['prefer_wkday_visit']=prefer_wkday_visit
df['prefer_wkend_purchase']=prefer_wkend_purchase
df['prefer_wkend_visit']=prefer_wkend_visit
df['price_rate']=price_rate
df['reg_date']=reg_date
df['top_genre_purchase']=top_genre_purchase
df['top_genre_visit']=top_genre_visit
df['usable_date_before_holiday']=usable_date_before_holiday
df['usable_fri']=usable_fri
df['usable_holiday']=usable_holiday
df['usable_mon']=usable_mon
df['usable_sat']=usable_sat
df['usable_sun']=usable_sun
df['usable_thu']=usable_thu
df['usable_tue']=usable_tue
df['usable_wed']=usable_wed
df['user_large_area']=user_large_area
df['valid_end']=valid_end
df['valid_from']=valid_from
df['valid_period']=valid_period
df['withdraw_date']=withdraw_date
df['is_male']=is_male

print '##### writing table with ids now... #####'
df.to_csv('test_wid.csv',encoding='utf-8',index=False)

df2 = pd.DataFrame() #table with user_id and coupon_id

df2['age']=age
df2['catelog_price']=catelog_price
df2['coupon_large_area']=coupon_large_area2
df2['discount_price']=discount_price
df2['disp_end']=disp_end
df2['disp_from']=disp_from
df2['disp_period']=disp_period
df2['distance']=distance2
df2['prefer_wkday_purchase']=prefer_wkday_purchase
df2['prefer_wkday_visit']=prefer_wkday_visit
df2['prefer_wkend_purchase']=prefer_wkend_purchase
df2['prefer_wkend_visit']=prefer_wkend_visit
df2['price_rate']=price_rate
df2['reg_date']=reg_date
df2['top_genre_purchase']=top_genre_purchase
df2['top_genre_visit']=top_genre_visit
df2['usable_date_before_holiday']=usable_date_before_holiday
df2['usable_fri']=usable_fri
df2['usable_holiday']=usable_holiday
df2['usable_mon']=usable_mon
df2['usable_sat']=usable_sat
df2['usable_sun']=usable_sun
df2['usable_thu']=usable_thu
df2['usable_tue']=usable_tue
df2['usable_wed']=usable_wed
df2['user_large_area']=user_large_area2
df2['valid_end']=valid_end
df2['valid_from']=valid_from
df2['valid_period']=valid_period
df2['withdraw_date']=withdraw_date
df2['is_male']=is_male
print '##### writing table without ids now... #####'
df2.to_csv('test_x.csv',encoding='utf-8',index=False)

df3 = pd.DataFrame()
df3['user_id']=user_id
df3['coupon_id']=coupon_id
df3.to_csv('test_ids.csv',encoding='utf-8',index=False)
