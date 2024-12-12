import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# Declare global variable
FILE_PATH = '/Users/aldomasendi/Documents/PELINDO/source/'
FILE_RESULT_PATH = '/Users/aldomasendi/Documents/PELINDO/result/'
FILE_NAME = 'container.csv'

# Read csv
df = pd.read_csv(os.path.join(FILE_PATH,FILE_NAME),sep=',')
container_data = df.loc[df['isKorean'] == True]
container_data['Date'] = pd.to_datetime(container_data['Date'])
container_data['Year'] = container_data['Date'].dt.year
container_data['Month'] = container_data['Date'].dt.month
container_data['Day'] = container_data['Date'].dt.day
container_data = container_data.drop(['Full_10','Empty_10','Full_40','Empty_40','Full_other','Empty_other'],axis=1)
container_data['total_capacity'] = container_data[['Full_20','Empty_20']].sum(axis=1)
container_data['max_capacity'] = container_data[['Full_20','Empty_20']].max(axis=1)
container_data['min_capacity'] = container_data[['Full_20','Empty_20']].min(axis=1)

# Calculate average container capacity per harbor per year
average_container = container_data[['Harbor','Year','total_capacity']].groupby(['Harbor','Year']).agg({'total_capacity':'mean'}).reset_index()

# Identify Harbor with the most large container capacity per year
sort_large_container_capacity = average_container.sort_values(['Year','total_capacity'],ascending=False)

# Visualization container capacity per month
container_capacity_monthly = container_data[['Harbor','Year','Month','total_capacity']].groupby(['Harbor','Year','Month']).agg({'total_capacity':'mean'}).reset_index()
container_capacity_monthly['year_month'] = container_capacity_monthly['Year'].astype(str) + '-' + container_capacity_monthly['Month'].astype(str)
container_capacity_monthly = container_capacity_monthly.set_index(pd.to_datetime(container_capacity_monthly['year_month']).rename('datetime')).reset_index()

trends = container_capacity_monthly.pivot_table('total_capacity',['datetime'],'Harbor').sort_values('datetime',ascending=True).reset_index()

plt.title('Trend container capacity per Harbor')
plt.style.use('fivethirtyeight')
plt.xticks(rotation=90)
plt.rcParams["figure.figsize"] = (35,10)
plt.plot(trends["datetime"], trends["Busan"],color='Blue', label='Busan')
plt.plot(trends["datetime"], trends["Daesan"],color='Red', label='Daesan')
plt.plot(trends["datetime"], trends["Incheon"],color='Green', label='Incheon')
plt.plot(trends["datetime"], trends["Gwangyang"],color='Purple', label='Gwangyang')
plt.plot(trends["datetime"], trends["Mokpo"],color='Yellow', label='Mokpo')
plt.plot(trends["datetime"], trends["Ulsan"],color='Orange', label='Ulsan')
plt.plot(trends["datetime"], trends["Pyeongtaek, Dangjin"],color='Black', label='Pyeongtaek, Dangjin')
plt.plot(trends["datetime"], trends["Pohang"],color='Pink', label='Pohang')
plt.plot(trends["datetime"], trends["Gunsan"],color='Brown', label='Gunsan')
plt.plot(trends["datetime"], trends["Gyeongin Port"],color='Gray', label='Gyeongin Port')
plt.plot(trends["datetime"], trends["Masan"],color='Cyan', label='Masan')
plt.plot(trends["datetime"], trends["East Sea, Mukho"],color='Indigo', label='East Sea, Mukho')
plt.legend(title="harbor",loc=4, fontsize='small', fancybox=True)
plt.savefig(os.path.join(FILE_RESULT_PATH,'pic.png'),bbox_inches='tight')
print(trends)

# Save the analysis to CSV
sort_large_container_capacity.to_csv(os.path.join(FILE_RESULT_PATH,'port_analysis_results.csv'),sep=',',index=False)
