import sys,os
sys.path.append(os.path.abspath("../scripts"))
sys.path.append(os.path.abspath("/Users/paulchamberlain/git/chamberpain-working/scripts"))
sys.path.append(os.path.abspath("/Users/paulchamberlain/Data/"))
import soccom_proj_settings
import pandas as pd
import itertools as it
import glob
time_format = '%m/%d/%Y'
import numpy as np
import soccom_proj_settings

SOCCOM_ids={'6091':'5904179','7557':'5904181','7567':'5904182','7613':'5904180','7614':'5904183',
'9091':'5904184','9092':'5904185','9031':'5904396','9018':'5904186','9095':'5904188','9101':'5904187',
'9254':'5904395','0037':'5904475','9313':'5904474','0508':'5904476','9096':'5904469','0509':'5904477',
'7652':'5904467','0511':'5904478','9094':'5904471','9275':'5904472','9099':'5904468','9125':'5904397',
'9260':'5904473','8514':'5904470','9668':'5904663','9666':'5904662','9646':'5904661','9652':'5904660',
'9657':'5904659','9655':'5904658','9662':'5904657','0506':'5904670','0507':'5904671','9749':'5904675',
'9645':'5904676','9757':'5904679','0564':'5904687','0510':'5904686','9602':'5904684','9637':'5904682',
'9650':'5904683','9600':'5904688','9631':'5904677','9744':'5904678','0570':'5904768','0568':'5904767',
'0565':'5904672','9761':'5904764','0571':'5904673','9265':'5904695','0566':'5904766','9660':'5904761',
'9632':'5904763','9634':'5904693','9762':'5904765','9630':'5904674','9752':'5904694','0068':'5903717',
'5146':'5901492','5426':'5902112','6968':'5903718','7552':'5903729','7619':'5904105','7620':'5904104',
'6091':'5904179'}


def skiplines_finder(file_):    #this algorythm iterates through the file until it comes to a specific formatting - all SOCCOM data begins in the text file with "CRUISE"
    with open(file_) as f:
        g = it.takewhile(lambda x: x[:6] != 'Cruise', f)
        skip_number = len([i for i, _ in enumerate(g)])+1
        return skip_number

def soccom_file_reader(file_):
    col = [u'Cruise', u'Date',  u'Lon', u'Lat', 'PosQC', u'Pressure','PressureQC',u'Temperature','TempQC',u'Salinity','SalQC', u'Oxygen','OxygenQC','OxygenSat','OxygenSatQC', u'Nitrate','NitrateQC','PH','PHQC','TALK','TALKQC','DIC','DICQC','pCO2','pCO2QC']   #initialize the column names
    skip_number = skiplines_finder(file_)   #get the number of lines to skip in the beginning of the record
    holder = pd.read_csv(file_, sep='\t',error_bad_lines=False,names = col, skiprows= skip_number, warn_bad_lines=False,usecols = [0,3,5,6,7,8,9,10,11,12,13,18,19,20,21,22,23,36,37,38,39,40,41,42,43], na_values='-1E+10')    #make a data frame and return it
    return holder

def soccom_df(data_directory):
    frames = []             #initialize the variable that we will append dataframes to 
    file_names = glob.glob(data_directory)   # develop list of files in the soccom data directory
    for float_name in file_names:
        print float_name
        frames.append(soccom_file_reader(float_name))           #pandas is orders of magnitude faster at appending then concatenating
    df_holder = pd.concat(frames)
    df_holder = df_holder[df_holder.Lat<-50]
    df_holder.Date = pd.to_datetime(df_holder.Date,format=time_format)  #make all datetime objects
    df_holder = df_holder.dropna(subset=['Pressure'])
    df_holder = df_holder.dropna(subset = ['Date'])     #drop bad dates (nothing can be done with this data)
    df_holder.loc[df_holder.PosQC==1,'Lon']=np.nan
    df_holder.loc[df_holder.PosQC==1,'Lat']=np.nan
    df_holder.loc[df_holder.PosQC==4,'PosQC']=8
    df_holder.loc[df_holder.PosQC==0,'PosQC']=1
    df_holder.loc[df_holder.PressureQC!=0,'Pressure']=np.nan
    df_holder.loc[df_holder.TempQC!=0,'Temperature']=np.nan
    df_holder.loc[df_holder.SalQC!=0,'Salinity']=np.nan
    df_holder.loc[df_holder.OxygenQC!=0,'Oxygen']=np.nan
    df_holder.loc[df_holder.OxygenSatQC!=0,'OxygenSat']=np.nan
    df_holder.loc[df_holder.NitrateQC!=0,'Nitrate']=np.nan
    df_holder.loc[df_holder.PHQC!=0,'PH']=np.nan
    df_holder.loc[df_holder.TALKQC!=0,'TALK']=np.nan
    df_holder.loc[df_holder.DICQC!=0,'DIC']=np.nan
    df_holder.loc[df_holder.pCO2QC!=0,'pCO2']=np.nan
    df_holder['Type']='SOCCOM'
    df_holder = df_holder.drop_duplicates(subset=['Cruise','Date','Pressure'])
    df_holder = df_holder.sort_values(['Cruise','Date','Pressure'])    #sort in a reasonable manner
    df_holder = df_holder.reset_index(drop=True)
    return df_holder


    #Still need to add functionality to select which data columns you want to read in
    #Still need to link to linear interpolator to interpolate using basemap
    #Still need to add HDF5 functionality
