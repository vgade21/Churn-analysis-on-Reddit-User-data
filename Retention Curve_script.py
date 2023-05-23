# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 20:04:55 2023

@author: vamsh
"""

import pandas
import pyarrow.feather as feather
import numpy

FilePath = "C:\\Users\\vamsh\\Documents\\Uni\\UChicago\\Winter 2023\\MSCA 37014_3 - Python for Analytics\\Assignments\\A4\\RC_2012_year_cohort.feather"

with open(FilePath, 'rb') as f:
    RC_2012_year_cohort_df = feather.read_feather(f)
    

RC_2012_year_cohort_df.head(10)

# Q1)
# Count the number of comments in each subreddit, sort that list and pick the top 30
n = 30
top_30_SubReddit = RC_2012_year_cohort_df['subreddit'].value_counts()[:n]
# Converting to dataframe
top_30_SubReddit_df = top_30_SubReddit.to_frame()
top_30_SubReddit_df.reset_index(inplace=True)
top_30_SubReddit_df = top_30_SubReddit_df.rename(columns={'index': 'SubReddit', 'subreddit': 'Count'})

'''
# The remaining steps will be done per-subreddit. The following logic will be applied in a function for 
# the remaining sub-reddit you are analyzing
# Q2)
# Filter the above dataframe so we're only looking at data form a single sub-reddit
# Test sub-reddit - Games
Games_SubReddit_df = RC_2012_year_cohort_df.loc[RC_2012_year_cohort_df['subreddit'] == 'guns']
Games_SubReddit_df.reset_index(inplace=True, drop=True)
# Dataframe which only contains 'author' and 'created_utc'
Games_SubReddit_df = Games_SubReddit_df.drop(['subreddit'], axis=1)

# Q3)
# Find the date of first interaction (for that sub-reddit)
Games_SubReddit_df = Games_SubReddit_df[['author', 'created_utc']]
Games_SubReddit_df["author"]=Games_SubReddit_df["author"].values.astype('string')
Games_SubReddit_df.dtypes
# Calculating the date on which an author posted their first comment in the sub-reddit
first_interaction = Games_SubReddit_df.loc[Games_SubReddit_df.groupby('author')['created_utc'].idxmin()].reset_index(drop=True)

# Q4)
# Combine the initial dataframe (Games_SubReddit_df) with the dataframe containing comment timestamps (first_interaction)
merge_df = pandas.merge(Games_SubReddit_df, first_interaction, how="inner", on=["author"])
# Dataframe with the author's name, the date of their first comment and the date of their current comment
merge_df = merge_df.rename({'created_utc_x': 'date_sec_current_comment', 'created_utc_y': 'date_sec_first_comment'}, axis=1)

#from datetime import datetime
# Calculating the number of days since the first comment
# Diff in days
# merge_df['date_diff_inDays_abs'] = abs(merge_df['date_sec_current_comment'] - merge_df['date_sec_first_comment'])/86400
# Diff in days - round
# merge_df['date_diff_inDays_round'] = round(abs(merge_df['date_sec_current_comment'] - merge_df['date_sec_first_comment'])/86400,0)
# merge_df.drop(['date_diff_inDays_round_down'], axis=1, inplace=True)
# Diff in days - round down
merge_df['date_diff_inDays'] = numpy.floor(abs(merge_df['date_sec_current_comment'] - merge_df['date_sec_first_comment'])/86400)

# Q5)
# Pivot the dataframe so the names of authors are on one axis and the days on which they commented 
# are on the second axis
# Note: entires aren't only 0 or 1. Each cell shows the number of comments made by user on that day.
# print(merge_df['date_diff_inDays'].max())
pivot_df = pandas.crosstab(merge_df['date_diff_inDays'], merge_df['author'], dropna=False)

# Q6)
# Sum the number of commenters for each day
chart_df = pivot_df.copy()
chart_df['Num_Com_per_Day'] = chart_df.gt(0).sum(axis=1)
chart_df.reset_index(inplace = True)

chart_df_2 = chart_df[['date_diff_inDays', 'Num_Com_per_Day']]

# Q7)
# Calculate the % of authors who returned after their first comment
chart_df_2["percent_auth_return"] = (chart_df_2["Num_Com_per_Day"] / chart_df_2['Num_Com_per_Day'][0]) * 100
    
# Q8)
# Display the Retention Curve
chart_df_3 = chart_df_2.copy()
chart_df_3['date_diff_inDays'] = chart_df_3['date_diff_inDays'].apply(numpy.int64)
chart_df_3['date_diff_inDays'] = 'Day ' + chart_df_3['date_diff_inDays'].astype(str)

import matplotlib.pyplot as plt
plt.figure(figsize=(7,7), dpi=1000)
plt.plot(chart_df_3['date_diff_inDays'], chart_df_3['percent_auth_return'], color='b', marker="x", markersize=4)  # Plot the chart
plt.xticks(['Day 0', 'Day 20', 'Day 40', 'Day 60', 'Day 80', 'Day 100', 'Day 120', 'Day 140', 'Day 160', 
            'Day 180', 'Day 200', 'Day 220', 'Day 240', 'Day 260', 'Day 280','Day 300', 
            'Day 320', 'Day 330'], rotation = 45, fontsize=7)
plt.xlabel('Days since First Interaction', fontsize=10)
plt.ylabel('% of Authors who returned after their First Comment', fontsize=10)
plt.title('Retention Curve')
plt.tight_layout()
plt.show()
'''

# Function to process all sub-reddits
candidate = top_30_SubReddit_df['SubReddit']
num = numpy.arange(0, 335, 1).tolist()
Days = 'Day ' + pandas.DataFrame(num).astype(str)

chart_df_4 = pandas.DataFrame()
chart_df_4['date_diff_inDays'] = Days

for sub in candidate:
    # The remaining steps will be done per-subreddit. The following logic will be applied in a function for 
    # the remaining sub-reddit you are analyzing
    # Q2)
    # Filter the above dataframe so we're only looking at data form a single sub-reddit
    # Test sub-reddit - Games
    SubReddit_df = RC_2012_year_cohort_df.loc[RC_2012_year_cohort_df['subreddit'] == sub]
    SubReddit_df.reset_index(inplace=True, drop=True)
    # Dataframe which only contains 'author' and 'created_utc'
    SubReddit_df = SubReddit_df.drop(['subreddit'], axis=1)
    
    # Q3)
    # Find the date of first interaction (for that sub-reddit)
    SubReddit_df = SubReddit_df[['author', 'created_utc']]
    SubReddit_df["author"]=SubReddit_df["author"].values.astype('string')
    SubReddit_df.dtypes
    # Calculating the date on which an author posted their first comment in the sub-reddit
    first_interaction = SubReddit_df.loc[SubReddit_df.groupby('author')['created_utc'].idxmin()].reset_index(drop=True)
    
    # Q4)
    # Combine the initial dataframe (SubReddit_df) with the dataframe containing comment timestamps (first_interaction)
    merge_df = pandas.merge(SubReddit_df, first_interaction, how="inner", on=["author"])
    # Dataframe with the author's name, the date of their first comment and the date of their current comment
    merge_df = merge_df.rename({'created_utc_x': 'date_sec_current_comment', 'created_utc_y': 'date_sec_first_comment'}, axis=1)
    
    #from datetime import datetime
    # Calculating the number of days since the first comment
    # Diff in days - round down
    merge_df['date_diff_inDays'] = numpy.floor(abs(merge_df['date_sec_current_comment'] - merge_df['date_sec_first_comment'])/86400)
    
    # Q5)
    # Pivot the dataframe so the names of authors are on one axis and the days on which they commented 
    # are on the second axis
    # Note: entires aren't only 0 or 1. Each cell shows the number of comments made by user on that day.
    # print(merge_df['date_diff_inDays'].max())
    pivot_df = pandas.crosstab(merge_df['date_diff_inDays'], merge_df['author'], dropna=False)
    
    # Q6)
    # Sum the number of commenters for each day
    chart_df = pivot_df.copy()
    chart_df['Num_Com_per_Day'] = chart_df.gt(0).sum(axis=1)
    chart_df.reset_index(inplace = True)
    
    chart_df_2 = chart_df[['date_diff_inDays', 'Num_Com_per_Day']]
    
    # Q7)
    # Calculate the % of authors who returned after their first comment
    chart_df_2[sub + "_percent_auth_return"] = (chart_df_2["Num_Com_per_Day"] / chart_df_2['Num_Com_per_Day'][0]) * 100
        
    # Q8)
    # Display the Retention Curve
    chart_df_3 = chart_df_2.copy()
    chart_df_3['date_diff_inDays'] = chart_df_3['date_diff_inDays'].apply(numpy.int64)
    chart_df_3['date_diff_inDays'] = 'Day ' + chart_df_3['date_diff_inDays'].astype(str)
    chart_df_3.drop(['Num_Com_per_Day'], axis=1, inplace=True)
    
    chart_df_4 = pandas.merge(chart_df_4, chart_df_3, how="left", on=["date_diff_inDays"])

chart_df_5 = chart_df_4.copy()
chart_df_5.drop(['date_diff_inDays'], axis=1, inplace=True)

import matplotlib.pyplot as plt
plt.figure(figsize=(7,7), dpi=1000)
for i, col in enumerate(chart_df_5.columns):
    plt.plot(chart_df_4['date_diff_inDays'], chart_df_4[col], label = candidate[i], linewidth=0.5)  # Plot the chart
plt.xticks(['Day 0', 'Day 20', 'Day 40', 'Day 60', 'Day 80', 'Day 100', 'Day 120', 'Day 140', 'Day 160', 
            'Day 180', 'Day 200', 'Day 220', 'Day 240', 'Day 260', 'Day 280','Day 300', 
            'Day 320', 'Day 334'], rotation = 45, fontsize=7)
plt.yticks(numpy.arange(0, 101, 5))
plt.xlabel('Days since First Interaction', fontsize=10)
plt.ylabel('% of Authors who returned after their First Comment', fontsize=10)
plt.title('Retention Curve for Top 30 Sub-reddits between January-2012 to November-2012')
plt.legend(loc='best', fontsize="small")
plt.grid()
plt.tight_layout()
plt.show()