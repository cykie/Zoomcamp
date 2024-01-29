#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[14]:


#pip install psycopg2-binary 


# In[2]:


taxi_zone=pd.read_csv('/Users/Cykie/taxi+_zone_lookup.csv')


# In[96]:


taxi_zone[taxi_zone['LocationID']==7]


# In[37]:


green_taxi=pd.read_csv('/Users/Cykie/green_tripdata_2019-09.csv')


# In[21]:


len(green_taxi)


# In[4]:


from sqlalchemy import create_engine


# In[38]:


green_taxi.lpep_pickup_datetime=pd.to_datetime(green_taxi.lpep_pickup_datetime)
green_taxi.lpep_dropoff_datetime = pd.to_datetime(green_taxi.lpep_dropoff_datetime )


# In[18]:





# In[16]:


print(pd.io.sql.get_schema(green_taxi,name='yellow_taxi_data',con=engine))


# In[15]:


engine=create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[29]:


df_iter=pd.read_csv('/Users/Cykie/green_tripdata_2019-09.csv',iterator=True, chunksize=100000)


# In[30]:


df=next(df_iter)


# In[31]:


df.lpep_pickup_datetime=pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime )


# In[32]:


len(df)


# In[33]:


get_ipython().run_line_magic('time', '')
df.to_sql("yellow_taxi_data",con=engine,if_exists='replace')


# In[34]:


from time import time


# In[35]:


while True:
    t_start=time()
    df=next(df_iter)
    df.lpep_pickup_datetime=pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime )
    df.to_sql("yellow_taxi_data",con=engine,if_exists='append')
    t_end=time()
    time_spent=t_end-t_start
 
    print("one chunk has been inserted, took %.3f second" % (time_spent))


# In[ ]:


green_taxi_1=green_taxi[['lpep_pickup_datetime','tip_amount']]


# In[64]:


green_taxt1=green_taxi[green_taxi['lpep_pickup_datetime'].dt.normalize()=='2019-09-18'][['lpep_pickup_datetime','PULocationID','total_amount']]

green_taxt1['lpep_pickup_datetime']=green_taxt1['lpep_pickup_datetime'].dt.normalize()


# In[69]:


merged_1=green_taxt1.merge(taxi_zone,left_on='PULocationID', right_on='LocationID')


# In[70]:


green_taxi_sort1=merged_1.groupby([merged_1['lpep_pickup_datetime'],'Borough']).sum('total_amount').reset_index()

green_taxi_sort2=green_taxi_sort1.sort_values('total_amount', ascending=False)

green_taxi_sort2


# In[71]:


green_taxi_2=green_taxi[['lpep_pickup_datetime','DOLocationID','PULocationID','tip_amount']]


# In[91]:


merged_2=green_taxi_2.merge(taxi_zone,left_on='PULocationID', right_on='LocationID',suffixes=("_1","_2"))

#merged_3=merged_2.merge(taxi_zone,left_on='DOLocationID', right_on='LocationID',suffixes=("_3","_4"))


# In[92]:


merged_2.head()


# In[109]:


merged_3=merged_2[merged_2['Zone']=='Astoria'][['DOLocationID','Zone','tip_amount']]
merged_4=merged_3.merge(taxi_zone,left_on='DOLocationID', right_on='LocationID',suffixes=("_1","_2"))            


# In[110]:


merged_4


# In[112]:


q2result=merged_4.groupby(['Zone_1','Zone_2']).sum('tip_amount').reset_index()
q2result.sort_values('tip_amount', ascending=False).head(20)


# In[ ]:




