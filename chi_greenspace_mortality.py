
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

URLS = [('https://data.cityofchicago.org/api/views/kn9c-c2s2/rows.csv?accessType=DOWNLOAD', 'Chicago_SES.csv'), 
        ('https://citytech-health-atlas-data-prod.s3.amazonaws.com/uploads/uploader/path/721/Green_Index__Land_Cover___Ave_Annual__v2.xlsx', 'Chicago_Green.xls'),
        ('https://data.cityofchicago.org/api/views/j6cj-r444/rows.csv?accessType=DOWNLOAD', 'Chicago_Death.csv'),
        ('https://data.cityofchicago.org/api/views/cjg8-dbka/rows.csv?accessType=DOWNLOAD', 'Chicago_health_cr.csv')]


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
#df = read_data(PATH, filemane)

def parse_pop(pop_df, date_range):
   df = pop_df[pop_df['Year'] == date_range]
   return df

df = parse_pop(pop_df, '2011-2015')

df.columns
#of interest: Geo_Group, Number






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
#'Community Areas' is now "Community Area (#)" and formatting is not ideal
def parse_healthcr(healthcr_df):
    healthcr_df['count_of_health_crs'] = 1 
    count_of_crs = healthcr_df.groupby('Community Area (#)').sum().reset_index()
    #count_of_crs.rename(columns = {'Community Area (#)':'CA'}, inplace = True)
    count_of_crs[['Community Area', 'Community Area Number']] = count_of_crs['Community Area (#)'].apply( 
   lambda x: pd.Series(str(x).split("(")))
    count_of_crs['Community Area Number'] = count_of_crs['Community Area Number'].apply(lambda x: pd.Series(str(x).strip(")")))
    count_of_crs['Community Area Number'] = count_of_crs['Community Area Number'].apply(lambda x: pd.Series(str(x).strip(" )")))
    count_of_crs['Community Area Number'] = count_of_crs['Community Area Number'].apply(lambda x: pd.Series(float(x)))
    
    #cite https://www.geeksforgeeks.org/split-a-text-column-into-two-columns-in-pandas-dataframe/
    return count_of_crs
#call
healthcr_df = read_data(PATH, 'Chicago_health_cr.csv')
crs = parse_healthcr(healthcr_df)
crs['Community Area Number'].dtype

#Merge datasets:
#updated to have pop -not tested
def merge_dfs(SES_df,green_df,avg_an_death,count_of_crs, pop):     
    SES_green = SES_df.merge(green_df, left_on='Community Area Number', right_on = 'Geo_ID', how = 'inner')

    SES_green_death = SES_green.merge(avg_an_death, on='Community Area Number', how = 'inner')

    #need to update, 'Community Area (#)' is the new column!
    SES_green_death_healthcr = SES_green_death.merge(count_of_crs, on='Community Area Number', 
                                                      how = 'outer')
    SES_green_death_healthcr_pop = SES_green_death_healthcr.merge(pop, on = 'Geo_Group', how = 'outer')
    #fill in Nan with 0 (bc if not in the previous database then doesn't have a health center)
    SES_green_death_healthcr_pop['count_of_health_crs'].fillna(value = 0, inplace=True)
    
    #drop colums with Nan (all cols dropped for this df are completely empty)
    SES_green_death_healthcr_pop.dropna(axis=1,inplace=True)

    #drop columns that do not provide useful information/may not apply to all entries in the row after 
    # the merge (e.g. SubCategory or Map_Key from green_df), or duplicates eg Geo_ID
    SES_green_death_healthcr_pop.drop(columns = ['Geo_Group', 'Geo_ID', 'Category', 'SubCategory',
                                            'Geography', 'Map_Key'], inplace = True)
    return SES_green_death_healthcr_pop 

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


#call the download funtion
#for url, filename in URLS:
#    download_data(url, filename)

def main():
    df_contents = []
    for url, filename in URLS:
        df = read_data(PATH, filename)
        if filename == 'Chicago_Death.csv':
            df_contents.append(parse_death(df))
        elif filename == 'Chicago_health_cr.csv':
            df_contents.append(parse_healthcr(df))
        else:
            df_contents.append(df) 
    df_contents.append(parse_pop(pop_df, '2011-2015'))
    #call the merge function
    full_df= merge_dfs(df_contents[0], df_contents[1], df_contents[2], df_contents[3],df_contents[4])
    #call the drop_col function -> generate primary df
    use_df = drop_col(full_df)
    #use_df = re_name(use_df)
    return use_df

use_df = main()
