import sys,os
sys.path.append(os.path.abspath("../"))

import soccom_proj_settings
import pandas as pd
import datetime
from soccom_read import soccom_df
from argo_read import argo_df

df = argo_df(soccom_proj_settings.argo_data_directory)

###  we remove these dates because they seem to be outlyers that are not physical
df = df[(df.Date!=datetime.date(2008,6,18))|(df.Cruise!=5901730)]
df = df[(df.Date!=datetime.date(2011,5,22))|(df.Cruise!=5901740)]
###############


df[['Cruise','PosQC']] = df[['Cruise','PosQC']].astype(int)
df.loc[df.Lon.values<0,['Lon']] = df[df.Lon<0].Lon.values+360
df = df[['Cruise','Date','Temperature','Salinity','Pressure','Lat','Lon','PosQC']]
df = df[(df.PosQC==1)|(df.PosQC==8)]
df.to_pickle(soccom_proj_settings.argo_drifter_file)

df_int = df.drop_duplicates(['Cruise','Date'])
df_int = df_int.sort_values(['Cruise','Date']).reset_index(drop=True)


for cruise in df_int.Cruise.unique():
	mask = df_int.Cruise==cruise
	df_int.loc[mask,'dt_interp'] = df_int[mask].PosQC==1
	df_int.loc[mask,'dt_interp'] = df_int[mask].dt_interp.apply(lambda x: 1 if x else 0).cumsum()
	df_holder = df_int[(df_int.PosQC==8)&(df_int.Cruise==cruise)]
	for g in df_holder.groupby('dt_interp').groups:
		frame = df_holder.groupby('dt_interp').get_group(g)
		dt = (frame.Date.max()-frame.Date.min()).days+2*df_int[mask].Date.diff().mean().days
		df_int.loc[df_int.index.isin(frame.index),'dt_interp']=dt
df_int.loc[df_int.PosQC==1,'dt_interp']=0


	# return plot_list

df_int.to_pickle(soccom_proj_settings.interpolated_drifter_file)
