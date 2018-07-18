import sys,os
import pandas as pd
import jdcal
import fnmatch
import time
import datetime
import numpy as np
from netCDF4 import Dataset
sys.path.append(os.path.abspath("../"))
import oceans
import matplotlib.pyplot as plt

debug = True
f = open('argo_df_changelog.txt','w')

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
    pos_type = ''.join([x for x in nc_fid.variables['POSITIONING_SYSTEM'][:][0].tolist() if x is not None])
    cruise = ''.join([x for x in nc_fid.variables['PLATFORM_NUMBER'][:][0].tolist() if x is not None])
    mode = nc_fid.variables['DATA_MODE'][:]
    frames = []
    for variable in np.unique(mode).data:
        truth_list = mode==variable
        if variable is 'R':
            lat = nc_fid.variables['LATITUDE'][truth_list].tolist()
            lon = nc_fid.variables['LONGITUDE'][truth_list].tolist()
            pos_qc = nc_fid.variables['POSITION_QC'][truth_list].tolist()
            date = nc_fid.variables['JULD'][truth_list].tolist()
            date_qc = nc_fid.variables['JULD_QC'][truth_list]
            sal = nc_fid.variables['PSAL'][truth_list,:].flatten().tolist()
            pressure = nc_fid.variables['PRES'][truth_list,:].flatten().tolist()
            temp = nc_fid.variables['TEMP'][truth_list,:].flatten().tolist()

            sal_error = nc_fid.variables['PSAL_ERROR'][truth_list,:].flatten().tolist()
            pressure_error = nc_fid.variables['PRES_ERROR'][truth_list,:].flatten().tolist()
            temp_error = nc_fid.variables['TEMP_ERROR'][truth_list,:].flatten().tolist()
            cycle = nc_fid.variables['CYCLE_NUMBER'][truth_list].tolist()

            sal_qc = nc_fid.variables['PSAL_QC'][truth_list,:].flatten().tolist()
            pressure_qc = nc_fid.variables['PRES_QC'][truth_list,:].flatten().tolist()
            temp_qc = nc_fid.variables['TEMP_QC'][truth_list,:].flatten().tolist()

        else:
            lat = nc_fid.variables['LATITUDE'][truth_list].tolist()
            lon = nc_fid.variables['LONGITUDE'][truth_list].tolist()
            pos_qc = nc_fid.variables['POSITION_QC'][truth_list].tolist() 
            try:
                date = nc_fid.variables['JULD_ADJUSTED'][truth_list].tolist()
                date_qc = nc_fid.variables['JULD_ADJUSTED_QC'][truth_list]
            except KeyError:
                date = nc_fid.variables['JULD'][truth_list].tolist()
                date_qc = nc_fid.variables['JULD_QC'][truth_list]
            sal = nc_fid.variables['PSAL_ADJUSTED'][truth_list,:].flatten().tolist()
            pressure = nc_fid.variables['PRES_ADJUSTED'][truth_list,:].flatten().tolist()
            temp = nc_fid.variables['TEMP_ADJUSTED'][truth_list,:].flatten().tolist()

            sal_error = nc_fid.variables['PSAL_ADJUSTED_ERROR'][truth_list,:].flatten().tolist()
            pressure_error = nc_fid.variables['PRES_ADJUSTED_ERROR'][truth_list,:].flatten().tolist()
            temp_error = nc_fid.variables['TEMP_ADJUSTED_ERROR'][truth_list,:].flatten().tolist()
            try:
                cycle = nc_fid.variables['CYCLE_NUMBER_ADJUSTED'][truth_list].tolist()
            except KeyError:
                cycle = nc_fid.variables['CYCLE_NUMBER'][truth_list].tolist()
            sal_qc = nc_fid.variables['PSAL_ADJUSTED_QC'][truth_list,:].flatten().tolist()
            pressure_qc = nc_fid.variables['PRES_ADJUSTED_QC'][truth_list,:].flatten().tolist()
            temp_qc = nc_fid.variables['TEMP_ADJUSTED_QC'][truth_list,:].flatten().tolist()
        dummy, col_num = nc_fid.variables['PSAL'][truth_list,:].shape

        lat = list_multiplier(lat,col_num)
        lon = list_multiplier(lon,col_num)
        pos_qc =  list_multiplier(pos_qc,col_num)
        cycle = list_multiplier(cycle,col_num)

        ref_date = ''.join(nc_fid.variables['REFERENCE_DATE_TIME'][:].tolist())
        ref_date = sum(jdcal.gcal2jd(ref_date[:4],ref_date[4:6],ref_date[6:8]))
        date = list_multiplier([item for sublist in [time_parser(x,ref_date) for x in date] for item in sublist],col_num)
        date_qc = list_multiplier(date_qc,col_num)

        frames.append(pd.DataFrame({'Cycle':cycle,'Date':date,'DateQC':date_qc,'Lon':lon,'Lat':lat,
            'Pressure':pressure,'Temperature':temp,'Salinity':sal,'PosQC':pos_qc,'PressureQC':pressure_qc,'Pressureerror':pressure_error,
            'SalQC':sal_qc,'Salerror':sal_error,'TempQC':temp_qc,'Temperror':temp_error}))

    df_holder = pd.concat(frames)

    df_holder['Type']=pos_type
    df_holder['Cruise']=cruise

    df_holder = df_holder.dropna(subset = ['Date'])
    df_holder = df_holder[(df_holder.PressureQC.isin(['1','2']))&(df_holder.SalQC.isin(['1','2']))&(df_holder.TempQC.isin(['1','2']))&
    (df_holder.PosQC.isin(['1','2','8']))&(df_holder.DateQC.isin(['1','2']))]

    if (df_holder.Salerror.values>0.5).any()|(df_holder.Pressureerror.values>20).any()|(df_holder.Temperror.values>0.5).any():
        # floats that are outside these parameters need to be eliminated because it is unclear what future data processing problems they may cause.
        print 'I found a float outside of error parameters'
        f.write(str(cruise[0])+' is rejected because it was found outside error parameters \n')
        nc_fid.close()
        return pd.DataFrame()
    nc_fid.close()
    return df_holder

def argo_df(data_directory):

    frames = []
    matches = []
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
    df_holder.Date = pd.to_datetime(df_holder.Date)
    df_holder = df_holder.dropna(subset = ['Pressure'])
    return df_holder

def traj_file_reader(file_):
    nc_fid = Dataset(file_, 'r')
    lat = nc_fid.variables['LATITUDE'][:].tolist()
    lon = nc_fid.variables['LONGITUDE'][:].tolist()
    num = nc_fid.variables['CYCLE_NUMBER'][:].tolist()
    pos_type = ''.join([x for x in nc_fid.variables['POSITIONING_SYSTEM'][:].tolist() if x is not None])
    pos_acc = nc_fid.variables['POSITION_ACCURACY'][:]
    pos_qc = nc_fid['POSITION_QC'][:]
    pos_acc = np.ma.array(pos_acc,mask=(pos_qc!='1'))

    date = nc_fid.variables['JULD'][:]
    date_qc = nc_fid.variables['JULD_QC'][:]
    date = np.ma.array(date,mask=(date_qc!='1'))

    cruise = ''.join([a for a in nc_fid['PLATFORM_NUMBER'][:].tolist() if a])
    ref_date = ''.join(nc_fid.variables['REFERENCE_DATE_TIME'][:].tolist())
    ref_date = datetime.datetime.strptime(ref_date,'%Y%m%d000000').date()
    date_list = []
    for n,day in enumerate(date):
        try:
            date_list.append(ref_date+datetime.timedelta(days=day))
        except TypeError:
            date_list.append(np.nan)
    # date = [item for sublist in [time_parser(x,ref_date) for x in date] for item in sublist]
    df_holder = pd.DataFrame({'Cruise':cruise,'Date':date_list,'Lon':lon,'Lat':lat,'Position Type':pos_type,'Position Accuracy':pos_acc,'Position QC':pos_qc})
    

    df_holder = df_holder.dropna(subset=['Date'])
    df_holder = df_holder[((df_holder['Position Type']=='ARGOS')&(df_holder['Position Accuracy'].isin(['1','2','3'])))|(df_holder['Position Type']=='GPS')]
    nc_fid.close()

    if df_holder.empty:
        return df_holder
    df_holder = df_holder.dropna(subset=['Lat','Lon'])
    df_holder = df_holder.drop_duplicates(subset=['Date'])
    df_holder.Date = pd.to_datetime(df_holder.Date)

    if  df_holder.Date.min()<pd.to_datetime(datetime.date(1996,1,1)):
        print 'huge problem'
        print df_holder.Date.min()
        plt.figure()
        plt.subplot(2,1,1)
        df_holder.set_index('Date').Lat.plot()
        plt.title('Latitude')
        plt.tick_params(
    axis='x',          # changes apply to the x-axis
    which='both',      # both major and minor ticks are affected
    bottom='off',      # ticks along the bottom edge are off
    top='off',         # ticks along the top edge are off
    labelbottom='off') # labels along the bottom edge are off
        plt.xlabel('')
        plt.subplot(2,1,2)
        df_holder.set_index('Date').Lon.plot()
        plt.title('Longitude')
        plt.savefig(cruise)
        plt.close()
        return pd.DataFrame()
    if not df_holder[(df_holder['Lat'].diff().abs()/df_holder.Date.diff().dt.days>.7)|((df_holder['Lon'].diff().abs()/df_holder.Date.diff().dt.days>.7)&(df_holder['Lon'].apply(oceans.wrap_lon180).diff().abs()/df_holder.Date.diff().dt.days>.7))].empty:
        # if not df_holder[(df_holder['Lat'].diff().abs()/df_holder.Date.diff().dt.days>.3)].empty:
        #     df_holder['Lat'] = pd.rolling_median(df_holder['Lat'], window=2, center=True).fillna(method='bfill').fillna(method='ffill')
        #     print 'I smoothed the lat'
        # if not df_holder[((df_holder['Lon'].diff().abs()/df_holder.Date.diff().dt.days>.3)&(df_holder['Lon'].apply(oceans.wrap_lon180).diff().abs()/df_holder.Date.diff().dt.days>.3))].empty:
        #     df_holder['Lon'] = pd.rolling_median(df_holder['Lon'], window=2, center=True).fillna(method='bfill').fillna(method='ffill')
        #     print 'I smoothed the lon'
        plt.figure()
        plt.subplot(2,1,1)
        df_holder.set_index('Date').Lat.plot()
        plt.tick_params(
    axis='x',          # changes apply to the x-axis
    which='both',      # both major and minor ticks are affected
    bottom='off',      # ticks along the bottom edge are off
    top='off',         # ticks along the top edge are off
    labelbottom='off') # labels along the bottom edge are off
        plt.xlabel('')
        plt.title('Latitude')
        plt.subplot(2,1,2)
        df_holder.set_index('Date').Lon.plot()
        plt.title('Longitude')
        plt.savefig(cruise)
        plt.close()
        return pd.DataFrame()
    return df_holder

def traj_df(data_directory):
    frames = []
    matches = []
    float_type = ['Argo']
    for root, dirnames, filenames in os.walk(data_directory):
        for filename in fnmatch.filter(filenames, '*Rtraj.nc'):
            matches.append(os.path.join(root, filename))
    for n, match in enumerate(matches):
        try:
            print 'file is ',match,', there are ',len(matches[:])-n,'floats left'
            t = time.time()
            frames.append(traj_file_reader(match))
            print 'Building and merging datasets took ', time.time()-t 
        except IndexError:
            continue
    df_holder = pd.concat(frames)
    df_holder.Date = pd.to_datetime(df_holder.Date)
    return df_holder