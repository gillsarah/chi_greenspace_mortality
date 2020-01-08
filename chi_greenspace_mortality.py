
import pandas as pd
import pandas_datareader.data as web
from pandas_datareader import wb
import requests
import os



PATH= '/Users/Sarah/Documents/GitHub/chi_greenspace_mortality'
os.chdir(os.path.expanduser(PATH)) #set working directory 
URL = 'https://citytech-health-atlas-data-prod.s3.amazonaws.com/uploads/uploader/path/557/Total_population.xlsx'
url = 'https://data.cityofchicago.org/api/views/kn9c-c2s2/rows.csv?accessType=DOWNLOAD' 
filemane = 'Chicago_SES.csv'

def download_data(url, filename):
    response = requests.get(url)
    if filename.endswith('.csv'):
        open_as = 'w'
        output = response.text
        #return open_as
        print('it was a csv') #debug
    elif filename.endswith('.xls'):
        open_as = 'wb'
        output = response.content
        #return open_as
        print('it was a xls') #debug
    else:
        return 'unexpected file type in download_data'
    
    with open(filename, open_as) as ofile:
        ofile.write(output)


#call
#download_data(URL, 'community_area_population.xls')
#download_data(url, filemane)


#read in data
def read_data(path, filename):
    if filename.endswith('.csv'):
        df = pd.read_csv(os.path.join(path, filename))
    elif filename.endswith('.xls'):
        df = pd.read_excel(os.path.join(path, filename))
    else:
        return 'unexpected file type in read_data'
    return df

#call
pop_df = read_data(PATH, 'community_area_population.xls')
df = read_data(PATH, filemane)

def parse_pop(pop_df):
    





#re-name community Area: for some reasion after the pivot, can't use Community Area to merge on
#but Community Area Number works 
#re-shape df so that numbers reflect average annual deaths

def parse_death(death_df):
    death_df.rename(columns = {'Community Area': 'Community Area Number'}, inplace=True)
    avg_an_death = death_df.pivot(index = 'Community Area Number', columns='Cause of Death', 
                                  values='Average Annual Deaths 2006 - 2010')
    avg_an_death.drop(0, axis = 0, inplace = True) #drop the Chicago Total
    return avg_an_death

#get a count of healthcare centers per community area:
def parse_healthcr(healthcr_df):
    healthcr_df['count_of_health_crs'] = 1 
    count_of_crs = healthcr_df.groupby('Community Areas').sum().reset_index()
    return count_of_crs

#Merge datasets:
def merge_dfs(SES_df,green_df,avg_an_death,count_of_crs):     
    SES_green = SES_df.merge(green_df, left_on='Community Area Number', right_on = 'Geo_ID', how = 'inner')

    SES_green_death = SES_green.merge(avg_an_death, on='Community Area Number', how = 'inner')

    SES_green_death_healthcr = SES_green_death.merge(count_of_crs, left_on='Community Area Number', 
                                                     right_on='Community Areas', how = 'outer')
    #fill in Nan with 0 (bc if not in the previous database then doesn't have a health center)
    SES_green_death_healthcr['count_of_health_crs'].fillna(value = 0, inplace=True)
    
    #drop colums with Nan (all cols dropped for this df are completely empty)
    SES_green_death_healthcr.dropna(axis=1,inplace=True)

    #drop columns that do not provide useful information/may not apply to all entries in the row after 
    # the merge (e.g. SubCategory or Map_Key from green_df), or duplicates eg Geo_ID
    SES_green_death_healthcr.drop(columns = ['Geo_Group', 'Geo_ID', 'Category', 'SubCategory',
                                            'Geography', 'Map_Key'], inplace = True)
    return SES_green_death_healthcr 

#cite: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.fillna.html

#reduce number of columns for convinience, drop ones outside of investigation
def drop_col(full_df):
    use_df = full_df.drop(columns = 
                        ['PERCENT OF HOUSING CROWDED',  'PERCENT AGED 16+ UNEMPLOYED',
                        'PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA', 'PERCENT AGED UNDER 18 OR OVER 64', 
                        'Year','Injury, unintentional', 'All causes in females', 'All causes in males', 
                        'Alzheimers disease', 'Assault (homicide)', 'Breast cancer in females', 
                        'Cancer (all sites)', 'Colorectal cancer', 'Prostate cancer in males',
                        'Firearm-related', 'Kidney disease (nephritis, nephrotic syndrome and nephrosis)', 
                        'Liver disease and cirrhosis', 'Lung cancer', 'Indicator'])
    return use_df
