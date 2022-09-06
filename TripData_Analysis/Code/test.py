import pandas as pd

#df = pd.read_csv(r'C:\Users\sbalas230\Documents\Test.csv')
df = pd.read_csv(r'C:\Users\sbalas230\Downloads\TripData.csv')
print(df['tpep_pickup_datetime'].max())
assert 2==3
print(df.columns)
print(len(df))

print(df['tip_amount'].mean())


df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
group = df.groupby(df.tpep_pickup_datetime.dt.hour)['total_amount'].sum().reset_index()
print(group)

print(type(df['total_amount'].max()))