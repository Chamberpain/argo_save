import pandas as pd
import glob
import numpy as np
from netCDF4 import Dataset 

def soccom_file_reader(file_token):
    frame_token = []
    col = [u'Pressure','Pressure_QF',u'Temperature','Temperature_QF',u'Salinity','Salinity_QF', u'Oxygen','Oxygen_QF','OxygenSat','OxygenSat_QF', u'Nitrate','Nitrate_QF','pH25C','pH25C_QF']   #initialize the column names
    nc_token = Dataset(file_token)
    cruise = ''.join(nc_token['Cruise'][:].tolist()).strip()
    for date_header in range(nc_token.dimensions['N_PROF'].size):
        date = ''.join(nc_token['mon_day_yr'][date_header].tolist())
        Lon = nc_token['Lon'][date_header]
        if np.abs(Lon)>360:
            print 'Longitude is showing some funny values for float ',cruise
        Lat = nc_token['Lat'][date_header]
        if np.abs(Lat)>90:
            print 'Latitude is showing some funny values for float ',cruise
        PosQC = nc_token['Lat_QF'][date_header]
        dataframe_token = pd.DataFrame()
        for variable in col: 
            dataframe_token[variable]=nc_token[variable][date_header]
        dataframe_token['Cruise']=cruise
        dataframe_token['Date']=pd.to_datetime(date)
        dataframe_token['Lat']=Lat
        dataframe_token['Lon']=Lon
        dataframe_token['PosQC']=PosQC
        frame_token.append(dataframe_token)
    holder = pd.concat(frame_token)
    return holder

def soccom_df(data_directory):
    frames = []             #initialize the variable that we will append dataframes to 
    file_names = glob.glob(data_directory)   # develop list of files in the soccom data directory
    for float_name in file_names:
        print float_name
        frames.append(soccom_file_reader(float_name))           #pandas is orders of magnitude faster at appending then concatenating
    df_holder = pd.concat(frames)
    df_holder = df_holder.dropna(subset=['Pressure'])
    df_holder = df_holder.dropna(subset = ['Date'])     #drop bad dates (nothing can be done with this data)
    df_holder.loc[df_holder.PosQC=='1','Lon']=np.nan
    df_holder.loc[df_holder.PosQC=='1','Lat']=np.nan
    df_holder.loc[df_holder.PosQC=='4','PosQC']=8
    df_holder.loc[df_holder.PosQC=='0','PosQC']=1
    df_holder.loc[df_holder.Pressure_QF!='0','Pressure']=np.nan
    df_holder.loc[df_holder.Temperature_QF!='0','Temperature']=np.nan
    df_holder.loc[df_holder.Salinity_QF!='0','Salinity']=np.nan
    df_holder.loc[df_holder.Oxygen_QF!='0','Oxygen']=np.nan
    df_holder.loc[df_holder.OxygenSat_QF!='0','OxygenSat']=np.nan
    df_holder.loc[df_holder.Nitrate_QF!='0','Nitrate']=np.nan
    df_holder.loc[df_holder.pH25C_QF!='0','pH25C']=np.nan
    df_holder['Type']='SOCCOM'
    df_holder = df_holder.drop_duplicates(subset=['Cruise','Date','Pressure'])
    df_holder = df_holder.sort_values(['Cruise','Date','Pressure'])    #sort in a reasonable manner
    df_holder = df_holder.reset_index(drop=True)
    df_holder = df_holder[['Date','Lat','Lon','Cruise','PosQC','Pressure','Temperature','Salinity','Oxygen','OxygenSat','Nitrate','pH25C']]
    return df_holder


    #Still need to add functionality to select which data columns you want to read in
    #Still need to link to linear interpolator to interpolate using basemap
    #Still need to add HDF5 functionality
