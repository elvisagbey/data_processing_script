# import modules
import easygui
import time
import warnings
import os
import pandas as pd
import numpy as np


# function to update data
def data_updater(current_df, previous_df):
    '''
    data_updater compares itemsets in current database to that of 
    previous database and returns a csv file
    '''

    # define dimensions
    dimensions = ['Period', 'Cycle', 'Country', 'Data Source', 'City_Region', 'Channel',
                  'CategoryName', 'SegmentName', 'Manufacturer', 'Brand', 'ItemName',
                  'Pack_Unit_Size']

    # set of countries
    countries = set(current_df['Country']).union(previous_df['Country'])

    # empty set to contain item set additions and substrations/ommissions
    item_set_additions = set()
    item_set_subtractions = set()

    # for each country, identify new item sets and update item_set_additions
    for country in countries:

        # filter previous and current dataframe for country
        previous_df_filtered = previous_df[previous_df['Country'] == country]
        current_df_filtered = current_df[current_df['Country'] == country]

        # perform pandas set operations to obtain items in current df but not
        # in previous df and update set
        item_set_additions.update(
            current_df_filtered[current_df_filtered['ItemName'].
                                isin(previous_df_filtered['ItemName']) == False]['ItemName'])

        # perform pandas set operations to obtain items in previous df but not
        # in current df and update set
        item_set_subtractions.update(
            previous_df_filtered[previous_df_filtered['ItemName'].
                                 isin(current_df_filtered['ItemName']) == False]['ItemName'])

    # empty dataframe to hold backward data
    df_backward = pd.DataFrame()

    # check for existence of new item sets
    if len(item_set_additions) != 0:

        # get dataframe of only item set additions
        df_additions = current_df.loc[current_df['ItemName'].isin(item_set_additions)]

        # replace all metric values with nan
        df_additions[[
            column for column in current_df.columns if column not in dimensions]] = np.nan
        # loop over previous periods to create backward data
        for period in previous_df['Period'].unique():
            df_additions.loc[:,'Period'] = period
            df_backward = pd.concat(
                [df_backward, df_additions], axis=0, ignore_index=True)

    # create empty dataframe to hold forward data
    df_forward = pd.DataFrame()

    # check for item set ommissions
    if len(item_set_subtractions) != 0:

        # query dataframe of item set ommissions
        df_subtractions = previous_df.loc[previous_df['ItemName'].isin(
            item_set_subtractions)]

        # set metric value of item set ommissions to nan
        df_subtractions[[
            column for column in previous_df.columns if column not in dimensions]] = np.nan

        # create forward data for item ommissions
        for period in current_df['Period'].unique():
            df_subtractions.loc[:,'Period'] = period
            df_forward = pd.concat(
                [df_forward, df_subtractions], axis=0, ignore_index=True)

    return df_backward, df_forward

# function to read data
def read_data(path, **kwargs):
    '''
    read_data takes a csv file path and returns a dataframe
    '''

    df = pd.read_csv(path, **kwargs) #encoding='iso-8859-1' removed from pd.read_csv

    df.dropna(axis=0, how='all', inplace=True)

    return df


# factory function
def run_data_updater():
    '''
    run_data_updater is factory function to run data_updater when script.py is run.
    '''

    warnings.filterwarnings("ignore")

    print('***** Maverick Retail Data Processor: By Wave-2 Analytics Ltd. *****')
    time.sleep(0.5)
    status = input('>> Proceed (Y/N)? ')
    if status.strip().lower() == 'n':
        os.abort()
    if status.strip().lower() == 'y':   
        print(">> upload previous dataset")
        time.sleep(1)
        path_prev = easygui.fileopenbox(msg = "Upload Previous Dataset",
                                        filetypes = "*.csv", multiple=True)

        print(">> uploading previous dataset ... ")
        try:
            previous_data = pd.concat([read_data(path) for path in path_prev], 
                                            ignore_index=True, axis=0)
        except pd.errors.ParserError:
            print('***** MESSAGE ***** \n>> wrong file extension. Restart and Upload csv')
            os.abort()
        except:
            print('***** MESSAGE ***** \n>> File upload failed')
            os.abort()
        else:
            print(">> data uploaded successfully")

        print(">> upload current dataset")
        time.sleep(1)
        path_cur = easygui.fileopenbox(msg = 'Upload Current Dataset',
                                        filetypes = "*.csv")
        print(">> uploading current dataset ...")
        try:
            current_data = read_data(path_cur)
            _period = current_data['Period'][0]
        except pd.errors.ParserError:
            print('***** MESSAGE ***** \n>> wrong file extension. Restart and Upload csv')
            os.abort()
        except:
            print('***** MESSAGE ***** \n>> File upload failed')
            os.abort()
        else:
            print(">> data uploaded successfully")
            df_backward, df_forward = data_updater(current_data, previous_data)
            if len(df_backward) == 0 and len(df_forward) == 0:
                print('***** MESSAGE ***** \n>> No dataset generated')
                os.abort()
            if os.path.exists('./data') == False:
                os.mkdir('./data')
            if len(df_backward) !=0:
                df_backward.to_csv(f'./data/backward_{_period}.csv', index=False) # encoding='iso-8859-1' commented out
                print(f'***** MESSAGE ***** \n>> backward_{_period}.csv \
                        generated and stored in \n{os.path.abspath("./data")}\n')
            if len(df_forward) !=0:
                df_forward.to_csv(f'./data/forward_{_period}.csv', index=False)  # encoding='iso-8859-1' commented out
                print(f'***** MESSAGE ***** \n>> forward_{_period}.csv \
                        generated and stored in \n{os.path.abspath("./data")}')


if __name__ == "__main__":
    run_data_updater()
