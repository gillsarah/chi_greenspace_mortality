#I should propbably start over with the data now avail on health-atlas!
#they have rates already!
#cardiovascular, veg index, per/capita income
#plus limited access to food
#or grocery stores per community area




import pandas as pd
import pandas_datareader.data as web
from pandas_datareader import wb
import requests
import os
import statsmodels.api as sm
import statsmodels.formula.api as smf 
import matplotlib.pyplot as plt


PATH= '/Users/Sarah/Documents/GitHub/chi_greenspace_mortality'
os.chdir(os.path.expanduser(PATH)) #set working directory 
URL = 'https://citytech-health-atlas-data-prod.s3.amazonaws.com/uploads/uploader/path/557/Total_population.xlsx'
#url = 'https://data.cityofchicago.org/api/views/kn9c-c2s2/rows.csv?accessType=DOWNLOAD' 
#filemane = 'Chicago_SES.csv'

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

#df = parse_pop(pop_df, '2011-2015')

#df.columns
#of interest: Geo_Group, Number






#re-name community Area: for some reasion after the pivot, can't use Community Area to merge on
#but Community Area Number works 
#re-shape df so that numbers reflect average annual deaths

def parse_death(death_df):
    death_df.rename(columns = {'Community Area': 'Community Area Number'}, inplace=True)
    avg_an_death = death_df.pivot(index = 'Community Area Number', columns='Cause of Death', 
                                  values='Average Annual Deaths 2006 - 2010')
    avg_an_death.drop(0, axis = 0, inplace = True) #drop the Chicago Total
    avg_an_death.reset_index(inplace = True)
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


def check_col(df):
    if 'Geo_ID' in(list(df.columns)):
        return 'Geo_ID'
    elif 'Community Area Number' in(list(df.columns)):
        return 'Community Area Number'
    else:
        print('column not found! merge will not be successful')

'Ave_Annual_Number' in (list(pop_df.columns))

'''
#calls: out of order!
SES_green = SES.merge(green, left_on= check_col(SES), right_on = check_col(green), how = 'outer')
SES_green.columns
SES_green.head()

green['Geo_ID'].dtype
SES['Community Area Number'].dtype

'Geo_ID' in(list(green.columns))
'Geo_ID' in(list(death.columns))
'Community Area Number' in(list(death.columns))

death.reset_index(inplace = True)
SES_green_death = SES_green.merge(death, left_on= check_col(SES_green), right_on = check_col(death), how = 'outer')
SES_green_death.shape

SES_green_death = SES_green.merge(death, on='Community Area Number', how = 'inner')

SES_green_death_healthcr = SES_green_death.merge(crs, on='Community Area Number', 
                                                      how = 'outer')
SES_green_death_healthcr.shape
summary_stats(SES_green_death_healthcr)

SES_green_death_healthcr_pop = SES_green_death_healthcr.merge(pop_df, on = 'Geo_Group', how = 'inner')
SES_green_death_healthcr_pop.shape

SES_green_death_healthcr_pop['count_of_health_crs'].fillna(value = 0, inplace=True)
'''
 

def summary_stats(df):
    summary = df.describe()
    #summary.drop(columns = ['Community_Area_Number'], inplace = True)
    summary = summary.transpose()
    summary = summary.round(2)
    return summary

def merge_dfs(SES_df,green_df,avg_an_death,count_of_crs, pop):     
    SES_green = SES_df.merge(green_df, left_on='Community Area Number', right_on = 'Geo_ID', how = 'inner')

    SES_green_death = SES_green.merge(avg_an_death, on='Community Area Number', how = 'inner')

    #need to update, 'Community Area (#)' is the new column!
    SES_green_death_healthcr = SES_green_death.merge(count_of_crs, on='Community Area Number', 
                                                      how = 'outer')
    SES_green_death_healthcr_pop = SES_green_death_healthcr.merge(pop, on = 'Geo_Group', how = 'outer', suffixes = ('', '_pop'))
    #fill in Nan with 0 (bc if not in the previous database then doesn't have a health center)
    SES_green_death_healthcr_pop['count_of_health_crs'].fillna(value = 0, inplace=True)
    
    #drop colums with Nan (all cols dropped for this df are completely empty)
    #no longer true, loseing a lot now
    #SES_green_death_healthcr_pop.dropna(axis=1,inplace=True)

    #drop columns that do not provide useful information/may not apply to all entries in the row after 
    # the merge (e.g. SubCategory or Map_Key from green_df), or duplicates eg Geo_ID
    #SES_green_death_healthcr_pop.drop(columns = ['Geo_Group', 'Geo_ID', 'Category', 'SubCategory',
    #                                        'Geography', 'Map_Key'], inplace = True)
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
'''
#call
SES = read_data(PATH, 'Chicago_SES.csv')
healthcr_df = read_data(PATH, 'Chicago_health_cr.csv')
crs = parse_healthcr(healthcr_df)
#crs['Community Area Number'].dtype
df = read_data(PATH, 'Chicago_Death.csv')
death = parse_death(df)
pop_df = parse_pop(pop_df, '2011-2015')

green = read_data(PATH, 'Chicago_Green.xls')

green.columns 
death.columns

full_df= merge_dfs(SES, green, death, crs, pop_df )
'''
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
    #use_df = drop_col(full_df)
    #use_df = re_name(use_df)
    return full_df

use_df = main()


#use_df = drop_col(use_df)
def re_name(df):
    df.rename(columns = {"Ave_Annual_Number": "Ave_Annual_Perc_Green"}, inplace = True)
    df.columns = [c.split(sep = ' (')[0] for c in df.columns]
    df.columns = [c.rstrip() for c in df.columns]
    df.columns = [c.replace(" ", "_") for c in df.columns]
    df.columns = [c.replace("-", "_") for c in df.columns]
    df.columns = [c.title() for c in df.columns]
    return df

use_df = re_name(use_df)
#use_df["Ave_Annual_Number"]

#Limitations in variation in greenspace (over half of the dataset is at 0 avg ann perc green!)
#look at a restricted sample, exclude 0 values.
def restricted_df(use_df):
    some_green = use_df['Ave_Annual_Perc_Green'] != 0
    some_green_df = use_df[some_green]
    some_green_df.reset_index(drop = True, inplace = True) #reset the index, needed for plotting later
    return some_green_df

#cite https://chrisalbon.com/python/data_wrangling/pandas_selecting_rows_on_conditions/  
#cite https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.reset_index.html

some_green_df = restricted_df(use_df)

def covt_check(df, y_string, x_string):
    print(y_string + ' on ' + x_string)
    test_model = smf.ols(y_string + '~' + x_string , data = df)
    result = test_model.fit()
    print(result.summary())
    print( )

covariate_check_list = [('Ave_Annual_Perc_Green', 'Hardship_Index'), 
                        ('Ave_Annual_Perc_Green', 'Per_Capita_Income'),
                        ('Ave_Annual_Perc_Green', 'Percent_Households_Below_Poverty')]
for i, j in covariate_check_list:
    covt_check(use_df, i, j)   


def ols(use_df, y):
    print('Dependent Variable: ' + y) #for output display
    m = smf.ols(y + '~ Ave_Annual_Perc_Green + Hardship_Index + Count_Of_Health_Crs' , data = use_df)
    result = m.fit()
    print(result.summary()) #show results in output
    print( ) #for output display readability
    #prepare results to save to png
    plt.rc('figure',figsize=(9, 5.5))
    plt.text(0.01, 0.05, str(result.summary()), {'fontsize': 10}, fontproperties = 'monospace')
    plt.axis('off')
    plt.tight_layout()
    plt.title('Death by '+ y +' on Green-Space, Controling for Area SES and Health Centers')


to_plot = [('Suicide','Average Anual Deaths by Suicide'), 
                ('Diabetes_Related','Average Anual Diabetes Related Deaths'),
                ('Coronary_Heart_Disease','Average Anual Deaths from Coronary Heart Disease'),
                ('Stroke','Average Anual Deaths by Stroke')]

for y, ylab in to_plot:
    ols(some_green_df, y)
    plt.savefig(y + '_reg_output.png')
    plt.close()


#plot: x:greenspace, y:avg anual deaths, size:per capita income in $1000
def death_green_SES_plot(df, y, ylabel):
    x = 'Ave_Annual_Perc_Green'
    z = df['Community_Area_Name']
    a_list = df['Per_Capita_Income']/1000
    ax = df.plot(kind='scatter', x=x, y= y , s = a_list, figsize= (10,8))
    #cite https://github.com/pandas-dev/pandas/issues/16827
    ax.set_ylabel(ylabel)
    ax.set_xlabel('Percent of Area that is Green')
    #lable point with Community_Area_Name, only if enough green or max death (for viewability) 
    temp_list = []
    for i, txt in enumerate(z): 
        if df[x][i] >= 2:
            ax.annotate(txt, (df[x][i], df[y][i]), horizontalalignment='center', verticalalignment='bottom')
        elif df[y][i] == df[y].max():
            temp_list.append(i)
            temp_list.append(txt)
            if len(temp_list) >=4:
                ax.annotate(temp_list[1], (df[x][temp_list[0]], df[y][temp_list[0]]), 
                            horizontalalignment='left', verticalalignment='bottom')
                ax.annotate(temp_list[3], (df[x][temp_list[2]], df[y][temp_list[2]]), 
                            horizontalalignment='left', verticalalignment='top')
            else:
                ax.annotate(temp_list[1], (df[x][temp_list[0]], df[y][temp_list[0]]), 
                            horizontalalignment='left', verticalalignment='bottom')
        else:
            pass   
    #cite: https://stackoverflow.com/questions/14432557/matplotlib-scatter-plot-with-different-text-at-each-data-point
    #https://matplotlib.org/3.1.0/gallery/text_labels_and_annotations/annotation_demo.html
    #remove spines
    ax.spines['right'].set_visible(False) 
    ax.spines['top'].set_visible(False)
    # Make a legend for per-capita income
    for a_list in [10, 20, 30]:
        plt.scatter([], [], c='k', alpha=0.3, s=a_list,
                    label=str(a_list) + 'k')
    plt.legend(scatterpoints=1, frameon=False, labelspacing=1, title='Per-Capita Income')
    #cite https://jakevdp.github.io/PythonDataScienceHandbook/04.06-customizing-legends.html
    #plt.savefig('SES_Green_and_Deaths_by_' +y)
    #plt.close()
    #plt.show()



for col, ylab in to_plot:
    death_green_SES_plot(some_green_df, col, ylab)
    plt.title('There is a Negative Relationship between Greenspace and Mortality Rate')
    plt.suptitle('Plot of Community Areas with Non-Zero Greenspace')
    plt.savefig('SES_Green_and_Deaths_by_' +col)
    plt.close()