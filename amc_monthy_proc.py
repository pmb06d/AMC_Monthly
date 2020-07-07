# -*- coding: utf-8 -*-
"""
Created on Mon May 11 17:24:11 2020

@author: Pedro
"""

import pandas as pd

# Pandas option to display up to 20 columns
pd.options.display.max_columns = 20
pd.options.display.max_rows = 250

# GUI pop-up to select a path     
def get_filepath():
    import tkinter
    from tkinter import filedialog
    
    root = tkinter.Tk()
    root.withdraw()
    return(filedialog.askopenfilename())


# Skipper function for IBOPE MW output
def skipper(file):
    with open(file) as f:
        lines = f.readlines()
        # get list of all possible lines starting with quotation marks
        num = [i for i, l in enumerate(lines) if l.startswith('"')]
        
        # if not found value return 0 else get first value of list subtracted by 1
        num = 0 if len(num) == 0 else num[0]
        return(num)
    
    
#test_csv = get_filepath()
#temp_file = pd.read_csv(test_csv,sep=',',nrows=skipper(test_csv),encoding='latin-1')
#temp_file = temp_file.iloc[:,1:]
#temp_file = temp_file.dropna(how='all')
#temp_file.tail()
#temp_file['Target'].unique()


#%%

ibope_dir = 'C:/Users/Pedro/Desktop/AMC Consulting/Monthly Report/IBOPE Runs'


# Reads in all the text files and compiles them into two dataframes
def data_reader(mypath):
    from os import listdir
    from os.path import isfile, join
    import pandas as pd
    from tqdm import tqdm
    import re
    
    # Parse the files to see how to process each one
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    
    region, dataset = [], []
    
    #print('\n','Compiling files for the current week:')
    for i in tqdm(onlyfiles):
        region.append(re.findall('^\w+',i)[0])
        filepath = mypath+'/'+i
        dataset.append(pd.read_csv(filepath,sep=',',nrows=skipper(filepath),encoding='latin-1'))
        
    #Add the country to each DF
    for data, country in zip(dataset, region):
        data['Country'] = country        
        
    return dataset


dataset = data_reader(ibope_dir)

#%%



# Clean numeric IBOPE variable columns
def clean_features(test_list, cleaned_features=False):
    import re
    
    temp = []
    temp2 = []
  
    for i in test_list:
        try:
            temp.append(re.findall('^(.+)\s\{',i)[0])
        except:
            temp.append(i)
      
    for i in test_list:
        if bool(re.match('^(.+)\s\{',i)) == True:
            temp2.append(re.findall('^(.+)\s\{',i)[0])
  
    temp = [re.sub('(?<=^[Mm]onth).+','',i) for i in temp] 
  
    if cleaned_features == True:
        return(temp2, temp)
    else:
        return(temp)
    
    
#clean_features(list(temp_file))      

# Target standarizer
def target_normalizer(target_list):
    import re
        
    age_regex = re.compile('[APWM][0-9\-\+]+.+')
    age_regex2 = re.compile('\w+\s*\-*Universe+')
    
    if type(target_list) is str:
        try:
            clean_str = age_regex.findall(target_list)[0].replace('04','')
        except:
            try:
                clean_str = (age_regex2.findall(target_list)[0].replace('-',' '))
            except:
                clean_str = target_list
                
        return(clean_str)
    
    else:        
        clean_values = []
        missing = []
            
        for item in target_list:
            try:
                clean_values.append(age_regex.findall(item)[0])
            except:
                try:
                    clean_values.append(age_regex2.findall(item)[0].replace('-',' '))
                except:
                    clean_values.append(item)
                    missing.append(item)
        
        if len(missing)==0:
            return(clean_values)
        else:
            return(clean_values)
            print('Could not normalize these targets:','\n')
            [print(i) for i in missing]
            
            
#target_normalizer(temp_file['Target'].unique())

# Clean numeric IBOPE variable columns
def to_numeric(feature_name,data):
    import pandas as pd
    data.loc[:,feature_name] = data.loc[:,feature_name].astype(str)
    data.loc[:,feature_name] = data.loc[:,feature_name].str.replace(r'^[Nn]\/*\.*[Aa]\.*[Nn]*$','0', regex=True)
    data.loc[:,feature_name] = data.loc[:,feature_name].str.replace(',','', regex=True)
    return(pd.to_numeric(data.loc[:,feature_name]))


def ats_to_min(ats_string):
    import re
    ATS_regex = re.compile('([0-9][0-9]?):([0-9][0-9]?):([0-9][0-9]?)')
    
    try:
        temp_list = ATS_regex.findall(ats_string)[0]
        temp_list = [int(i) for i in temp_list]
        return(temp_list[0]*60 + temp_list[1] + temp_list[2]/60)
    except:
        return(ats_string)

#test_for_ats = temp_file.copy()
#test_for_ats = pd.DataFrame(test_for_ats.loc[:,'Ats'])
#test_for_ats['Ats_New'] = temp_file['Ats'].apply(ats_to_min)
#test_for_ats.loc[test_for_ats['Ats_New'].isna(),:]

# General pre-processing for channel rankers
def preproc_ranker(dataframe_list, export_ranking_features=True, convert_date=True, exclude_floc=True):
    import pandas as pd
    from tqdm import tqdm 

    clean_df_list = []
    for temp_df in tqdm(dataframe_list):
        try:
            temp_df.drop([' '], axis=1, inplace=True)
        except:
            pass
        
        temp_df.loc[:,'Target'] = temp_df['Target'].str.replace('Live / ', '', regex=True)
        temp_df.loc[:,'Channel'] = temp_df['Channel'].str.replace(' (MF)', '', regex=False)
        temp_df.loc[:,'Channel'] = temp_df['Channel'].str.replace('_MF', '', regex=False)
        facts, all_columns = clean_features(list(temp_df), cleaned_features=True)
        temp_df.columns = clean_features(all_columns)
        temp_df = temp_df.dropna(how='all')
    
        if exclude_floc == True:
            temp_df['Channel_lower'] = temp_df['Channel'].str.lower()
            temp_df['Filter'] = temp_df['Channel_lower'].str.contains('floc', regex=False)
            temp_df = temp_df.loc[temp_df['Filter']==False,:]
            temp_df = temp_df.drop(['Filter','Channel_lower'], axis=1)
        
        clean_df_list.append(temp_df)

    temp_df = pd.concat(clean_df_list)    


    for col in facts:
        if col == 'Ats':
            temp_df['Ats_mins'] = temp_df['Ats'].apply(ats_to_min)
            temp_df['Ats_mins'] = temp_df['Ats_mins'].str.replace('n.a','0')
            temp_df['Ats_mins'] = pd.to_numeric(temp_df['Ats_mins'])
            #continue
        else:
            try:
                temp_df.loc[:,col] = to_numeric(col,temp_df)
            except:
                temp_df.loc[:,col] = to_numeric(col,temp_df.loc[:,col].str.replace('n.a','0', regex=False).replace(',','', regex=False))
            
    for col in all_columns:
        if col == 'Date' and convert_date == True:
            temp_df.loc[:,col] = pd.to_datetime(temp_df.loc[:,col])
        #elif col == 'Year':
        #    temp_df.loc[:,col] = temp_df.loc[:,col].astype('int')
        else:
            continue
    
    info = pd.DataFrame(temp_df.dtypes).reset_index()
    info = list(info.loc[info[0]!='float64','index'])
    info.remove('Channel')
    info.remove('Ats')
    
    temp_df.loc[:,info] = temp_df.loc[:,info].fillna('[TOTAL]')
    
    temp_df = temp_df.replace(to_replace=r'\[TOTAL\].+', value='[TOTAL]', regex=True)
    temp_df = temp_df.replace(to_replace=r'.*YTD.*', value='YTD', regex=True)
    
    
    if export_ranking_features == True:
        return(info, temp_df)
    else:
        return(temp_df)
    
#%%

#ranking_features, channel_rankers_preproc = preproc_ranker(temp_file)

ranker_df = dataset.copy()

# Pre-process and add a ranking variable
def process_ranker(ranker_df):
    import pandas as pd
    ranking_features, channel_rankers_preproc = preproc_ranker(ranker_df)
    
    # add the channel categories
    try:
        categories = pd.read_excel('C:/Users/Pedro/Desktop/AMC Consulting/Monthly Report/IBOPE Channel Reference.xlsx')
    except:
        path = get_filepath()
        categories = pd.read_excel(path)
    
    channel_rankers_preproc.loc[:,'Channel'] = channel_rankers_preproc['Channel'].str.replace('*','',regex=False)
    channel_rankers_preproc = channel_rankers_preproc.merge(categories, how='left', left_on='Channel', right_on='MW_Name')

    # check for missing
    missing = list(channel_rankers_preproc.loc[channel_rankers_preproc['Category1'].isna(),'Channel'].unique())
    if len(missing) == 0:
        pass
    else:
        print('There are channels missing in the excel file:')
        [print(i) for i in missing]    
        
    exclude = ['Children','Virtual']
    
    channel_rankers_preproc = channel_rankers_preproc.loc[~(channel_rankers_preproc['Category1'].isin(exclude)),:]
    
    channel_rankers_preproc['Rank_Rat%'] = channel_rankers_preproc.groupby(ranking_features)['Rat%'].rank(ascending=False,method='first')
    
    channel_rankers_preproc.loc[:,'Target'] = channel_rankers_preproc['Target'].apply(target_normalizer)
    
    return(channel_rankers_preproc)


#%%
# this weeks channel rankers
channel_rankers_df = process_ranker(temp_file)

#ranking_features, channel_rankers_preproc = preproc_ranker(temp_file)
#for label in ranking_features:
#    print(label,channel_rankers_df[label].unique(),'\n')


#channel_rankers_df.head()

# test for the ranker
channel_rankers_df.loc[(channel_rankers_df['Target']=='Pay Universe')
                       &(channel_rankers_df['TimeBand']=='06:00:00 - 30:00:00')
                       &(channel_rankers_df['Period']=='YTD')
                       &(channel_rankers_df['Month Name']=='[TOTAL]'),:].sort_values(by=['Rat%'], ascending=False).head(10)


output_path = 'C:/Users/Pedro/Desktop/AMC Consulting/Monthly Report/'

#stack
#channel_rankers_df = channel_rankers_df.append(channel_rankers_bench,sort=False)

#output
cr_output = output_path+'Test AMC Channel Rankers.csv'
channel_rankers_df.to_csv(path_or_buf=cr_output, sep=',', index=False)