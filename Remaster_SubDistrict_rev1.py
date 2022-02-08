from h3 import h3
from datetime import datetime, date, timedelta
from math import radians, cos, sin, asin, sqrt
from geopandas import GeoDataFrame
import geopandas as gpd
from shapely.geometry import Polygon, mapping
import pyproj    #to convert coordinate system
from shapely.geometry import Point
from csv_join_tambon import Reverse_GeoCoding, Reverse_GeoCoding_CenterGrid, Reverse_GeoCoding_5km2
from Credential import *
import networkx as nx
import osmnx as ox
import numpy as np
import os
import ast
import fiona
import pandas as pd
import pickle
import glob
from sys import exit
import warnings
import requests
import swifter
import matplotlib as mpl
import matplotlib.pyplot as plt
from tqdm import *
import seaborn as sns
fp = mpl.font_manager.FontProperties(family='Tahoma',size=13)
warnings.filterwarnings('ignore')

#enable tqdm with pandas, progress_apply
tqdm.pandas()

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))

###################################################################
sns.set(style='whitegrid', palette='pastel', color_codes=True)
sns.mpl.rc('figure', figsize=(10,6))
# plt.rcParams['font.family']='tahoma'
plt.rc('font',family='tahoma')
############################################################################################## 
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def GetH3hex(lat,lng,h3_level):
    return h3.geo_to_h3(lat, lng, h3_level)

def GetCenterHex_Lat(hex_id):
    return h3.h3_to_geo(hex_id)[0]

def GetCenterHex_Lng(hex_id):
    return h3.h3_to_geo(hex_id)[1]

def RemoveSpaceInString(x):
    return x.replace(' ','')
#### Reverse geocoing with Longdo map
def ReverseGeocoding_Longdo(lat,lng):
    # Use URL from opendata website
    url = 'https://api.longdo.com/map/services/address?'  
    stringSearch='lon=%s&lat=%s&noelevation=1&key=%s'%(lng,lat,longdo_api)
    url=url+stringSearch
    #print(' url : ',url)

    response = requests.get(url)
    
    try:
        result=response.json()
        output_string=result['road']+' '+result['subdistrict']+' '+result['district']+' '+result['province']+' '+result['country']+' '+result['geocode']
    except:
        output_string=''
    #print(' result :: ', output_string)
    return output_string

### Search location by keyword
def SearchLocation_Longdo(keyword, lon,lat, span):
    # Use URL from opendata website
    url = 'https://search.longdo.com/mapsearch/json/search?'  
    stringSearch='keyword=%s&lon=%s&lat=%s&span=%s&key=%s'%(keyword,lon,lat,span,longdo_api)
    url=url+stringSearch
    #print(' url : ',url)

    response = requests.get(url)
    
    try:
        result1=response.json()
        result=result1['data'] 
        #print(' result : ',type(result))
        outputDf=pd.DataFrame()        
        for n in result:
            row_append={ 'id':n['id'], 'name':n['name'],'lat':n['lat'],'lng':n['lon'],'icon':n['icon'],'address':n['address'],'obsolated':n['obsoleted'],'distance':n['distance']  }
            outputDf=outputDf.append( row_append, ignore_index=True).reset_index(drop=True)
    except:
        outputDf=pd.DataFrame()

    return outputDf

def Read_SHAPE_File(file_path,sub_dir,file_name):
    
    # Read file using gpd.read_file()
    #data = gpd.read_file(file_path+"TH_amphure.shp")
    # data1 = gpd.read_file(file_path+sub_dir+file_name, encoding = "iso-8859-1")  #ISO-8859-1
    data1 = gpd.read_file(file_path+sub_dir+file_name, encoding = "utf-8")  #ISO-8859-1    
    data1 = data1.to_crs(epsg=4326)
    #print(' gpd : ',data['a_name_t'].head(10))
    print(' gpd : ',data1.head(10))

    return data1

def MapValue_Value(x,dictIn):
    try:
        mapped=dictIn[x]
    except:
        #print('==> ',x)
        mapped=x
    return mapped

def Generate_Geopandas(dfIn, latCol, lngCol):
    dfIn['coords']=dfIn.progress_apply(lambda x: Pack_Coor(x[latCol],x[lngCol]) ,axis=1)
    print(len(dfIn), '*********** ---- Geopandas Generation---- ',dfIn.head(5),' :: ',dfIn.columns)
    crs = {'init':'EPSG:4326'}
    geometry = [Point(xy) for xy in zip(dfIn[lngCol], dfIn[latCol])]
    return gpd.GeoDataFrame(dfIn,   crs = crs,  geometry = geometry)

def Generate_Geopandas_2(dfIn, latCol, lngCol):
    dfIn['coords']=dfIn.progress_apply(lambda x: Pack_Coor(x[latCol],x[lngCol]) ,axis=1)
    print(len(dfIn), '*********** ---- Geopandas Generation---- ',dfIn.head(5),' :: ',dfIn.columns)
    crs = {'init':'EPSG:4326'}  
    geomList=[]
    for hexId in tqdm(dfIn['hex_id']): 
        geomList.append(Generate_HexGeometry_2(hexId))
    geometry=geomList
    return gpd.GeoDataFrame(dfIn,   crs = crs, geometry=geometry)

def Generate_Geometry_4326(latList, lngList):        
    crs = {'init':'EPSG:4326'}
    geometry = Polygon(zip(lngList, latList))
    return geometry

def Generate_HexGeometry_2(hexId):
    dummyString=str(h3.h3_to_geo_boundary(hexId))
    # print(' ---> ',dummyString, ' : ',type(dummyString))
    words=['((','))']
    for word in words:
        dummyString =  dummyString.replace(word, "").strip()
    stringList=dummyString.split('), (')
    # print(' list : ',stringList)
    latList=[]; lngList=[]
    for component in stringList:
        latList.append(float(component.split(',')[0]))
        lngList.append(float(component.split(',')[1]))
    # print(latList, ' :: ',lngList)
    return Generate_Geometry_4326(latList, lngList)

def Generate_new_boundary(file1_name, file2_name):

    dfIn1=pd.read_excel(current_path+'\\'+file1_name)
    dfIn1['Source']='102901'
    dfIn2=pd.read_excel(current_path+'\\'+file2_name)
    dfIn2['Source']='102902'

    dfIn=pd.DataFrame()
    dfIn=dfIn1.append(dfIn2).reset_index(drop=True)
    includeList=['Source','new_latlng']
    dfIn=dfIn[includeList].copy()
    print(len(dfIn),' ---> ',dfIn,' :: ',dfIn.columns)
    del dfIn1, dfIn2

    def Separate_Lat_Lng(listIn):
        latList=[]
        lngList=[]
        count=0
        for element in listIn:
            count=count+1
            print(count,' ==> ',element)
            latList.append(float(element.strip().split(',')[1]))
            lngList.append(float(element.strip().split(',')[0]))
        return latList, lngList


    listIn=list(dfIn['Source'].unique())

    mainDf=pd.DataFrame(listIn,columns=['Source'])

    geometryList=[]
    for idIn in listIn:
        dfDummy=dfIn[dfIn['Source']==idIn].copy().reset_index(drop=True)
        print(len(dfDummy),' ---- dummy ---- ',dfDummy.head(3), ' :: ',dfDummy.columns)
        locationList=list(dfDummy['new_latlng'])
        # print(' ==> ',locationList)
        latList, lngList=Separate_Lat_Lng(locationList)
        print(latList, ' :: ',lngList)
        geometry=Generate_Geometry_4326(latList, lngList)
        print('geometry  : ',geometry)
        geometryList.append(geometry)

        del  dfDummy
        
    crs = {'init':'EPSG:4326'}
    gdf_main=gpd.GeoDataFrame(mainDf,   crs = crs, geometry=geometryList)    
    print(len(gdf_main),' ---- main ----- ',gdf_main.head(5),' :: ',gdf_main.columns)
    del dfIn
    return gdf_main
########################################################################################################
######  Input ----  ####################################################################################
# level 8 covers approx 1 km2 (0.73 km2)
h3_level=8   

# working directory
current_path=os.getcwd()
print(' -- current directory : ',current_path)  # Prints the current working directory
boundary_path=current_path+'\\raw_data\\boundary_data\\'
shape_path='\\raw_data\\SHAPE\\'
new_shape_path='\\raw_data\\BMASubDistrict_Polygon\\'
input_path=current_path+'\\'

original_subdistrict_file='TH_tambon_boundary.shp'
new_subdistrict_file='BMA_ADMIN_SUB_DISTRICT.shp'

# input filename
input_name=''     ### Location   798, 800, 807, 817
file1_name='102901_location_update1_edited.xlsx'
file2_name='102902_location_update4_edited.xlsx'

#output filename
output_name='updated_TH_tambon_boundary.shp'   #### Location
#######################################################################################################

#### original 
sub_dir=shape_path
originalDf=Read_SHAPE_File(current_path,sub_dir,original_subdistrict_file)
print(len(originalDf),' originalDf : ',originalDf, '  :: ',type(originalDf), ' :: ',originalDf.columns)
# originalDf.to_excel(current_path+'\\'+'check_original.xlsx',index=False)

extractDf=originalDf[originalDf['t_name_e'].isin(['BANG SUE'])].copy().reset_index(drop=True)
p_name_e=list(extractDf['p_name_e'].values)[0]
a_name_e=list(extractDf['p_name_e'].values)[0]
p_code=list(extractDf['p_code'].values)[0]
a_code=list(extractDf['a_code'].values)[0]

processDf=originalDf[~originalDf['t_name_e'].isin(['BANG SUE'])].copy().reset_index(drop=True)
print(len(processDf),' processDf : ',processDf, '  :: ',type(processDf), ' :: ',processDf.columns)

##########################################################################################################
#### Update
sub_dir=new_shape_path
newDf=Read_SHAPE_File(current_path,sub_dir,new_subdistrict_file)
print(len(newDf),' newDf : ',newDf, '  :: ',type(newDf), ' :: ',newDf.columns)
# newDf.to_excel(current_path+'\\'+'check_new.xlsx',index=False)

## select only new records
dfDummy=newDf[newDf['SUBDISTRIC'].isin(['102901','102902'])].copy().reset_index(drop=True)
dfDummy.drop(columns=['OBJECTID','AREA_CAL','AREA_BMA','PERIMETER','ADMIN_ID','Shape_Leng'],inplace=True)
dfDummy.rename(columns={'SUBDISTRIC':'tambon_idn','DISTRICT_I':'amphoe_idn','CHANGWAT_I':'prov_id','Shape_Area':'area_sqm','CHANGWAT_N':'p_name_t','DISTRICT_N':'a_name_t','SUBDISTR_1':'t_name_t'},inplace=True)

subdistrict_map={'102901':'BANG SUE','102902':'WONGSAWANG'}
dfDummy['t_name_e']=dfDummy.apply(lambda x: MapValue_Value(x['tambon_idn'],subdistrict_map), axis=1)
dfDummy['a_name_e']=a_name_e
dfDummy['p_name_e']=p_name_e
dfDummy['t_code']=dfDummy['tambon_idn'].apply(lambda x: x[4:len(x)])
dfDummy['p_code']=p_code
dfDummy['a_code']=a_code
dfDummy['s_region']='R1'
dfDummy['BS_IDX']=''
print(' dfDummy : ',dfDummy, '  :: ',type(dfDummy), ' :: ',dfDummy.columns)
# dfDummy.to_excel(current_path+'\\'+'check_dummy.xlsx',index=False)

gdf_main=Generate_new_boundary(file1_name, file2_name)
gdf_main.rename(columns={'Source':'tambon_idn'},inplace=True)
print(' main : ',gdf_main)

dfDummy.drop(columns=['geometry'],inplace=True)
dfDummy=dfDummy.merge(gdf_main, on='tambon_idn', how='left')

processDf=processDf.append(dfDummy).reset_index(drop=True)
print(len(processDf),' processDf 2 : ',processDf, '  :: ',type(processDf), ' :: ',processDf.columns)

processDf.to_file(current_path+'\\'+output_name, encoding='utf-8') 
###########################################################################################################

del originalDf, extractDf, newDf, dfDummy, processDf
###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')