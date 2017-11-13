import sys,os
import pandas as pd
import jdcal
import fnmatch
import time
from netCDF4 import Dataset
import datetime
import numpy as np



debug = True

def time_parser(date,ref_date):
    try:
        x = jdcal.jd2gcal(date,ref_date)
        year = x[0]
        month = x[1]
        day = x[2]
        return [datetime.date(year,month,day)]
    except:
        return [np.nan]


def list_multiplier(list_,col_num):
    list_ = [[x]*col_num for x in list_]
    list_ = [item for sublist in list_ for item in sublist]
    return list_

def argo_file_reader(file_):
    nc_fid = Dataset(file_, 'r')
    lat = nc_fid.variables['LATITUDE'][:].tolist()
    lon = nc_fid.variables['LONGITUDE'][:].tolist()
    pos_qc = nc_fid.variables['POSITION_QC'][:].tolist()

    # sal_qc = nc_fid.variables['']
    date = nc_fid.variables['JULD'][:].tolist()

    sal = nc_fid.variables['PSAL_ADJUSTED'][:].flatten().tolist()
    pressure = nc_fid.variables['PRES_ADJUSTED'][:].flatten().tolist()
    temp = nc_fid.variables['TEMP_ADJUSTED'][:].flatten().tolist()

    sal_error = nc_fid.variables['PSAL_ADJUSTED_ERROR'][:].flatten().tolist()
    pressure_error = nc_fid.variables['PRES_ADJUSTED_ERROR'][:].flatten().tolist()
    temp_error = nc_fid.variables['TEMP_ADJUSTED_ERROR'][:].flatten().tolist()
        


    try:
        sal_qc = nc_fid.variables['PSAL_ADJUSTED_QC'][:].flatten().tolist()
        pressure_qc = nc_fid.variables['PRES_ADJUSTED_QC'][:].flatten().tolist()
        temp_qc = nc_fid.variables['TEMP_ADJUSTED_QC'][:].flatten().tolist()
    except AttributeError:
        sal_qc = nc_fid.variables['PSAL_QC'][:].flatten().tolist()
        pressure_qc = nc_fid.variables['PRES_QC'][:].flatten().tolist()
        temp_qc = nc_fid.variables['TEMP_QC'][:].flatten().tolist()        


    row_num, col_num = nc_fid.variables['PSAL'][:].shape
    cruise = [file_.split('/')[-2]]*row_num*col_num

    lat = list_multiplier(lat,col_num)
    lon = list_multiplier(lon,col_num)
    pos_qc =  list_multiplier(pos_qc,col_num)


    ref_date = ''.join(nc_fid.variables['REFERENCE_DATE_TIME'][:].tolist())
    ref_date = sum(jdcal.gcal2jd(ref_date[:4],ref_date[4:6],ref_date[6:8]))
    date = list_multiplier([item for sublist in [time_parser(x,ref_date) for x in date] for item in sublist],col_num)

    df_holder = pd.DataFrame({'Cruise':cruise,'Date':date,'Lon':lon,'Lat':lat,'Pressure':pressure,'Temperature':temp,'Salinity':sal,'PosQC':pos_qc,'PressureQC':pressure_qc,'Pressureerror':pressure_error,'SalQC':sal_qc,'Salerror':sal_error,'TempQC':temp_qc,'Temperror':temp_error})
    df_holder = df_holder.dropna(subset = ['Date'])
    df_holder = df_holder[(df_holder.PressureQC=='1')&(df_holder.SalQC=='1')&(df_holder.TempQC=='1')]
    
    if (df_holder.Salerror.values>0.5).any()|(df_holder.Pressureerror.values>20).any()|(df_holder.Temperror.values>0.5).any():
        print 'I found a float outside of error parameters'
        mask = (df_holder.Salerror<0.1)&(df_holder.Pressureerror<15)&(df_holder.Temperror.values<0.1)
        df_holder = df_holder[mask]
    nc_fid.close()
    return df_holder

def argo_df(data_directory):
    frames = []
    matches = []
    float_type = ['Argo']
    for root, dirnames, filenames in os.walk(data_directory):
        for filename in fnmatch.filter(filenames, '*prof.nc'):
            matches.append(os.path.join(root, filename))
    for n, match in enumerate(matches):
        print 'file is ',match,', there are ',len(matches[:])-n,'floats left'
        t = time.time()
        frames.append(argo_file_reader(match))
        if debug:
            print 'Building and merging datasets took ', time.time()-t 
    df_holder = pd.concat(frames)
    df_holder['Type']=float_type*len(df_holder)
    df_holder.Date = pd.to_datetime(df_holder.Date)
    df_holder = df_holder.dropna(subset = ['Pressure'])
    return df_holder

def traj_file_reader(file_):
    nc_fid = Dataset(file_, 'r')
    lat = nc_fid.variables['LATITUDE'][:].tolist()
    lon = nc_fid.variables['LONGITUDE'][:].tolist()
    num = nc_fid.variables['CYCLE_NUMBER'][:].tolist()
    pos_type = ''.join([x for x in nc_fid.variables['POSITIONING_SYSTEM'][:].tolist() if x is not None])
    pos_acc = nc_fid.variables['POSITION_ACCURACY'][:].tolist()
    date = nc_fid.variables['JULD'][:].tolist()
    cruise = file_.split('/')[-2]
    print nc_fid.variables['REFERENCE_DATE_TIME'][:]
    ref_date = ''.join(nc_fid.variables['REFERENCE_DATE_TIME'][:].tolist())
    ref_date = sum(jdcal.gcal2jd(ref_date[:4],ref_date[4:6],ref_date[6:8]))
    date = [item for sublist in [time_parser(x,ref_date) for x in date] for item in sublist]

    df_holder = pd.DataFrame({'Cruise':cruise,'Date':date,'Lon':lon,'Lat':lat,'Position Type':pos_type,'Position Accuracy':pos_acc})
    nc_fid.close()
    return df_holder


def traj_df(data_directory):
    frames = []
    matches = []
    float_type = ['Argo']
    for root, dirnames, filenames in os.walk(data_directory):
        for filename in fnmatch.filter(filenames, '*Rtraj.nc'):
            matches.append(os.path.join(root, filename))
    for n, match in enumerate(matches):
        print 'file is ',match,', there are ',len(matches[:])-n,'floats left'
        t = time.time()
        frames.append(traj_file_reader(match))
        print 'Building and merging datasets took ', time.time()-t 
    df_holder = pd.concat(frames)
    df_holder.Date = pd.to_datetime(df_holder.Date)
    df_holder = df_holder.dropna(subset = ['Pressure'])
    return df_holder


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