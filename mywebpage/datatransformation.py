# # import huspacy
# # # huspacy.download()
# # import hu_core_news_lg
# # nlp = hu_core_news_lg.load()
# from datetime import datetime
# import pandas as pd
# import os
# from sqlalchemy import create_engine, text as sql_text
# import re
# from collections import Counter
# import numpy as np
# from collections import defaultdict


# def datatransformation_for_chartjs(year, month, day, hour, minutes, seconds, year_end, month_end, day_end, hour_end, minutes_end, seconds_end,frequency):

#   ######################################
#   #   FETCHING DATA FROM ELEPHANTSQL   #
#   ######################################
    
#   database_url = "postgres://omeakqpt:xUUfVIvuZMNPUookJJXGiq4vFAwcShil@flora.db.elephantsql.com/omeakqpt"
#   database_url=database_url.replace('postgres', 'postgresql')
#   engine = create_engine(database_url)
#   sql_query_tracking = sql_text('SELECT * FROM "existinguser"')
#   with engine.connect() as connection:
#     result = connection.execute(sql_query_tracking)
#     rows = result.fetchall()
#   columns = result.keys()
  

#   # path="test_finalmodule.xlsx"
#   # df_pandas = pd.read_excel(path)

  
#   #######################################################
#   #   CREATING AND RESTRUCTURING THE PANDAS DATAFRAME   #
#   #######################################################

#   df_pandas = pd.DataFrame(rows, columns=columns)

#   # Remove milliseconds from 'created_at' column and set it as index
#   df_pandas['created_at'] = pd.to_datetime(df_pandas['created_at']).dt.floor('s')

#   # Set 'created_at' column as the index
#   df_pandas.set_index('created_at', inplace=True)
#   df_pandas.drop(columns=['id'], inplace=True)
#   df_pandas['user_id'] = df_pandas['user_id'].apply(lambda x: x.split('_')[0])
#   df_pandas.index = pd.to_datetime(df_pandas.index)

#   # Using the extracted data from the FORM to get the requested PERIOD 

#   from_date={"year":year, "month":month, "day":day, "hour":hour, "minutes":minutes, "seconds":seconds}
#   to_date={"year":year_end, "month":month_end, "day":day_end, "hour":hour_end, "minutes":minutes_end, "seconds":seconds_end}
  
#   def create_date_time(date):
#     date_time_obj = datetime(year=int(date["year"]), month=int(date["month"]), day=int(date["day"]),
#                             hour=int(date["hour"]), minute=int(date["minutes"]), second=int(date["seconds"]))

#     # Format the datetime object as a string
#     formatted_date_time = date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
#     return formatted_date_time

#   from_=create_date_time(from_date)
#   to_=create_date_time(to_date)

#   df_pandas = df_pandas.sort_index()
#   df_pandas=df_pandas.loc[from_: to_]

# ###########################################################################################
# #  df_pandas DATAFRAME CONTAINING THE CHAT MESSAGES IN THIS WAY:                          #
# #                                                                                         #
# #    created_at          user_id             message                                      #
# #  2024-03-03 21:07:39  127.0.0.1  USER: Milyen akusztikus gitárok kaphatók? | AS...      #
# #  2024-03-03 21:08:24  127.0.0.1  USER: Milyen ceruzatartók kaphatók? | ASSISTAN...      #
# ###########################################################################################

#   # Using the requested FREQUENCY (daily, weekly, monthly, yearly) to the following breakdown of the data

#   def find_day_of_week(date_string):
#       try:
#           # Convert the date string to a datetime object
#           date_object = datetime.strptime(date_string, '%Y-%m-%d')
          
#           # Get the day of the week (Monday is 0 and Sunday is 6)
#           day_of_week = date_object.weekday()
          
#           # Define a list of days of the week
#           days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
          
#           # Return the day of the week corresponding to the index
#           return days[day_of_week]
#       except ValueError:
#           return "Please enter a valid date string in the format YYYY-MM-DD"
      
#   timestamp=[]
#   breakdown=frequency

#   ##############
#   #   WEEKLY   #
#   ##############

#   if breakdown=="weekly":
#     # Convert index to datetime if it's not already
#     df_pandas.index = pd.to_datetime(df_pandas.index)

#     # Find the start and end dates of the data
#     start_date = df_pandas.index.min().date()  # Extract the date component
#     end_date = df_pandas.index.max().date()    # Extract the date component

#     # Initialize a list to store sub DataFrames
#     sub_dataframes = []

#     # Defining a function to get the start and end dates of a week
#     def get_week_boundaries(date):
#         start_of_week = date - pd.DateOffset(days=date.weekday())
#         end_of_week = start_of_week + pd.DateOffset(days=6)
#         return start_of_week.date(), end_of_week.date()  # Convert Timestamp to date

#     # Iterate over weeks from start to end
#     current_date = start_date
#     while current_date <= end_date:
#         # Get the start and end dates of the current week
#         week_start, week_end = get_week_boundaries(pd.Timestamp(current_date))
#         # Select rows within the current week
#         sub_df = df_pandas[(df_pandas.index.date >= week_start) & (df_pandas.index.date <= week_end)]
#         # Append the sub DataFrame to the list
#         if not sub_df.empty:
#           sub_dataframes.append(sub_df)
#         # Move to the next week
#         current_date = (week_end + pd.DateOffset(days=1)).date()  # Ensure current_date is a date object
#     # Creating timestamp for the charts (I take the last time of the period for each block)
#     for i in sub_dataframes:
#       timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))

#   ##############
#   #   DAILY    #
#   ##############

#   if breakdown=="daily":
#     # Convert index to datetime 
#     df_pandas.index = pd.to_datetime(df_pandas.index)

#     # Finding the start and end dates of the data
#     start_date = df_pandas.index.min().date()  # Extract the date component
#     end_date = df_pandas.index.max().date()    # Extract the date component

#     # Initialize a list to store sub DataFrames
#     sub_dataframes = []

#     # Iterate over each day from start to end
#     current_date = start_date
#     while current_date <= end_date:
#         # Select rows for the current day
#         sub_df = df_pandas[df_pandas.index.date == current_date]
#         # Append the sub DataFrame to the list
#         sub_dataframes.append(sub_df)
#         # Move to the next day
#         current_date += pd.Timedelta(days=1)

#     for i in sub_dataframes:
#       timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))

#   ##############
#   #   YEARLY   #
#   ##############

#   if breakdown=="yearly":
#     # Convert index to datetime if it's not already
#     df_pandas.index = pd.to_datetime(df_pandas.index)

#     # Find the start and end dates of the data
#     start_year = df_pandas.index.min().year  # Extract the year component
#     end_year = df_pandas.index.max().year    # Extract the year component

#     # Initialize a list to store sub DataFrames
#     sub_dataframes = []

#     # Iterate over each year from start to end
#     for year in range(start_year, end_year + 1):
#         # Select rows for the current year
#         sub_df = df_pandas[df_pandas.index.year == year]
#         # Append the sub DataFrame to the list
#         sub_dataframes.append(sub_df)

#     for i in sub_dataframes:
#       timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))

#   ###############
#   #   MONTHLY   #
#   ###############

#   if breakdown=="monthly":
#     # Convert index to datetime if it's not already
#     df_pandas.index = pd.to_datetime(df_pandas.index)

#     # Find the start and end dates of the data
#     start_date = df_pandas.index.min().date()  # Extract the date component
#     end_date = df_pandas.index.max().date()    # Extract the date component

#     # Initialize a list to store sub DataFrames
#     sub_dataframes = []

#     # Convert end_date to Timestamp for comparison
#     end_date_timestamp = pd.Timestamp(end_date)

#     # Iterate over each month from start to end
#     current_date = pd.Timestamp(start_date)  # Convert current_date to Timestamp
#     while current_date <= end_date_timestamp:
#         # Get the start and end dates of the current month
#         start_of_month = pd.Timestamp(year=current_date.year, month=current_date.month, day=1)
#         end_of_month = start_of_month + pd.offsets.MonthEnd(0)
#         # Select rows for the current month
#         sub_df = df_pandas[(df_pandas.index >= start_of_month) & (df_pandas.index <= end_of_month)]
#         # Append the sub DataFrame to the list
#         sub_dataframes.append(sub_df)
#         # Move to the next month
#         current_date = end_of_month + pd.Timedelta(days=1)
    
#     for i in sub_dataframes:
#       timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))


#   # database_url = os.environ.get('DATABASE_URL')

#   ##################################################################################################################################
#   #   READ EXCEL FILE AS "DF" PANDAS DATAFARME WHICH CONTAINS THE PRODUCTS ON STOCK IN A FORMAT (IN HUNGARIAN):                    #
#   #                                                                                                                                #
#   #   PRODUCT       TYPE                MANUFACTURER      BRAND NAME        AVAILABILITY      PRICE          DESCRIPTION           #
#   #    guitar      classical guitar        Ibanez          IbanezS540         on stock        160000 HUF    Hollow body...         #
#   #    piano        concert piano          Bösendorfer    Bösendorfer Grand   on stock     150000000 HUF    Extraordinary sound... #
#   ##################################################################################################################################

#   path="tesztexcel_hangszer.xlsx"
#   df = pd.read_excel(path)
#   df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
#   df.fillna("####", inplace=True)

#   # Hungarian stopwords
#   stopwords="""a
#   abban
#   ahhoz
#   ahogy
#   ahol
#   aki
#   akik
#   akkor
#   alatt
#   amely
#   amelyek
#   amelyekben
#   amelyeket
#   amelyet
#   amelynek
#   ami
#   amikor
#   amit
#   amolyan
#   amíg
#   annak
#   arra
#   arról
#   az
#   azok
#   azon
#   azonban
#   azt
#   aztán
#   azután
#   azzal
#   azért
#   be
#   belül
#   benne
#   bár
#   cikk
#   cikkek
#   cikkeket
#   csak
#   de
#   e
#   ebben
#   eddig
#   egy
#   egyes
#   egyetlen
#   egyik
#   egyre
#   egyéb
#   egész
#   ehhez
#   ekkor
#   el
#   ellen
#   első
#   elég
#   elő
#   először
#   előtt
#   emilyen
#   ennek
#   erre
#   ez
#   ezek
#   ezen
#   ezt
#   ezzel
#   ezért
#   fel
#   felé
#   hanem
#   hiszen
#   hogy
#   hogyan
#   igen
#   ill
#   ill.
#   illetve
#   ilyen
#   ilyenkor
#   ismét
#   ison
#   itt
#   jobban
#   jó
#   jól
#   kell
#   kellett
#   keressünk
#   keresztül
#   ki
#   kívül
#   között
#   közül
#   legalább
#   legyen
#   lehet
#   lehetett
#   lenne
#   lenni
#   lesz
#   lett
#   maga
#   magát
#   majd
#   majd
#   meg
#   mellett
#   mely
#   melyek
#   mert
#   mi
#   mikor
#   milyen
#   minden
#   mindenki
#   mindent
#   mindig
#   mint
#   mintha
#   mit
#   mivel
#   miért
#   most
#   már
#   más
#   másik
#   még
#   míg
#   nagy
#   nagyobb
#   nagyon
#   ne
#   nekem
#   neki
#   nem
#   nincs
#   néha
#   néhány
#   nélkül
#   olyan
#   ott
#   pedig
#   persze
#   rá
#   s
#   saját
#   sem
#   semmi
#   sok
#   sokat
#   sokkal
#   szemben
#   szerint
#   szinte
#   számára
#   talán
#   tehát
#   teljes
#   tovább
#   továbbá
#   több
#   ugyanis
#   utolsó
#   után
#   utána
#   vagy
#   vagyis
#   vagyok
#   valaki
#   valami
#   valamint
#   való
#   van
#   vannak
#   vele
#   vissza
#   viszont
#   volna
#   volt
#   voltak
#   voltam
#   voltunk
#   által
#   általában
#   át
#   én
#   éppen
#   és
#   így
#   ő
#   ők
#   őket
#   össze
#   úgy
#   új
#   újabb
#   újra"""

#   lista=[]
#   for index, row in df.iterrows():
#     lista.append(row['termék'].strip()) 
#     lista.append(row['típus'].strip())
#     lista.append(row['gyártó'].strip())
#     lista.append(row['márka'].strip())

#   # extract all the unique word from the DF dataframe like "guitar, Ibanez, piano etc."
#   lista=list(set(lista))
#   music_list = [value for value in lista if value == value]  # KEYWORDS (PRODUCTS, TYPES, MANUFACTURES, BRAND NAMES)
  
# #######################################################################################################################################
# # data_to_transform LIST WILL DETAIL KEYWORDS(GUITAR, IBANEZ ETC.) CLASSIFIED INTO SUB AND MAIN CATEGORIES ON THE BASIS OF DF_PANDAS  #
# # AND IT SUMS THE TOTAL NUMBER OF THEM IN A WAY LIKE THIS (sublists organized as: [0] product,  [1] type, [2] manufacturer, [3] brand #                                                                            #
# # [[{'guitar': 10}], [{'classical': 1}, {'electrical': 5}], [{'ortega': 1}, {'ibanez': 6}], [{'ortega ocapouke-wnd': 1}, {'dsl': 1}]] #
# # "guitar" can be more then  the sum of "manufactrers"(ortega, ibanez) as in chats they can turn up more times than the manufacturer  #
# #######################################################################################################################################

#   data_to_transform=[]



# ###################################################################################################################
# #                    ------------------- STARTING ROW OF THE BOTTLENECK ------------------------                  #
# ################################################################################################################### 
# #----------------------------------------------------------
# # FIRST BIG PART OF THE BOTTLENECK 
# # There are 3 parts here: CLEANING, CREATING LEMMATIZATION,
# # REGEX. REGEX TAKES THE LONGEST TIME, 2 OR 3 TIMES
# # LONGER THAN LEMMATIZATION AND CLEANING IS REALLY TINY AMOUNT 
# # I TRIED WITH DIFFERENT TREE DATASTRUCTURES FOR REGEX, BUT DIDN'T 
# # IN REDUCING THE PROCESS TIME
# #---------------------------------------------------------

#   for df_pandas in sub_dataframes:    # sub_dataframes contains each period a week, month etc.
#     topic_list = []

#     ########################################
#     # WE START iterating over the MESSAGES # 
#     ########################################

#     # Cleaning the messages (DELETING STOPWORDS, AND LEAVING ONLY THE LEMMAS)
#     for index, row in df_pandas.iterrows():
#       if not pd.isna(row['message']):
#         text=(row['message'])
#       cleaned=[]
#       temp=[]
#       #CLEANING
#       for k in text.split():
#         if k.lower() not in stopwords:
#           temp.append(k)
#       cleaned.append(" ".join(temp))
#       #LEMMATIZATION
#       text=[]
#       for i in cleaned:
#         doc=i#nlp(i)
#         lemmas=[]
#         for token in doc:
#           lemmas.append(token.lemma_)
#         text.append(' '.join(lemmas))

#       text=" ".join(text).lower()    # MESSAGE WITHOUT STOPWORDS AND INFLECTIONS
    
#       list_extraction = []    # CONTAINS THE KEYWORDS FOUND IN EACH MESSAGE

#       # REGEX: finding the keywords in the messages

#       for i in music_list:
#           # Define the pattern using regular expression
#           pattern = r"\b" + re.escape(i.lower()) + r"\b"

#           # Search for the pattern in the text
#           matches = re.findall(pattern, text.lower())

#           if matches:
#               # Iterate over the matches and add them to the list_extraction
#               for match in matches:
#                   list_extraction.append(match.strip())

#       list_extraction=list(set(list_extraction))
      


  

#     ###############################################################################################################
#     #  Match the keywords/message with the excel sheet and identify the product line, type, manufacturer, brand   #
#     ###############################################################################################################
#     #------------------------------------
#     # SECOND BIG PART OF THE BOTTLENECK -
#     #------------------------------------
      
#       # Let's check if the product line name (first column in excel) can be found in the chat and what corresponding types
#       finalization_list=[]                                       # manufacturer, brand can also be detected
#       # finalization_list contains a detected categories in the chat (product, type, manufacturer, brand)
#       for p in list_extraction:
#         for index, row in df.iterrows():
#           if not pd.isna(row['termék']):      #PRODUCT
#             if p==row['termék'].strip().lower():
#               temporary_list=[]
#               temporary_list.append([p])
#               for i in list_extraction:
#                 if not pd.isna(row['típus']):      #TYPE
#                   if i==row['típus'].strip().lower() and len(temporary_list)==1:
#                     temporary_list.append([row['termék'], row['típus']])
#                 if not pd.isna(row['gyártó']):      #MANUFACTURER
#                   if i==row['gyártó'].strip().lower() and len(temporary_list)>1:
#                     temporary_list.append([row['termék'], row['típus'], row['gyártó']])
#                   if i==row['gyártó'].strip().lower() and len(temporary_list)==1:
#                     temporary_list.append([row['termék'], "típust nem említett", row['gyártó']])
#                 if not pd.isna(row['márka']):    #BRAND
#                   if i==row['márka'].strip().lower():
#                     temporary_list.append([row['termék'], row['típus'], row['gyártó'], row['márka']])
#               longest_element = max(temporary_list, key=len)
#               finalization_list.append(longest_element)

#           #Let's do the same check in case there is no PRODUCT(main category) in the chat but TYPE can be found
#           if not pd.isna(row['típus']):
#             if p==row['típus'].strip().lower():
#               temp_list2=[]
#               for i in list_extraction:
#                 if not pd.isna(row['termék']):
#                   if i==row['termék'].strip().lower():
#                     temp_list2.append(i)

#               if len(temp_list2)==0:
#                 temporary_list=[]
#                 temporary_list.append([row['termék'], row['típus']])
#                 for i in list_extraction:
#                   if not pd.isna(row['gyártó']):
#                     if i==row['gyártó'].strip().lower():
#                       temporary_list.append([row['termék'], row['típus'], row['gyártó']])
#                   if not pd.isna(row['márka']):
#                     if i==row['márka'].strip().lower():
#                       temporary_list.append([row['termék'], row['típus'], row['gyártó'], row['márka']])


#                 longest_element = max(temporary_list, key=len)
#                 finalization_list.append(longest_element)

#           #Let's do the same check in case there is no PRODUCT(main category) and TYPE in the chat but MANUFACTURER can be found
#           if not pd.isna(row['gyártó']):
#             if p==row['gyártó'].strip().lower():
#               temp_list3=[]
#               for i in list_extraction:
#                 if not pd.isna(row['termék']):
#                   if i==row['termék'].strip().lower():
#                     temp_list3.append(i)
#                 if not pd.isna(row['típus']):
#                   if i==row['típus'].strip().lower():
#                     temp_list3.append(i)
#               if len(temp_list3)==0:
#                 temporary_list=[]
#                 temporary_list.append(["Terméket nem említett", "Típust nem említett", row['gyártó']])
#                 for i in list_extraction:
#                   if not pd.isna(row['márka']):
#                     if i==row['márka'].strip().lower():
#                       temporary_list.append([row['termék'], row['típus'], row['gyártó'], row['márka']])
#                 longest_element = max(temporary_list, key=len)
#                 finalization_list.append(longest_element)

#           #Let's do the same check in case there is no PRODUCT(main category) TYPE, MANUFACTURER in the chat but BRAND can be found

#           if p==row['márka'].strip().lower():
#             temp_list4=[]
#             for i in list_extraction:
#               if not pd.isna(row['termék']):
#                 if i==row['termék'].strip().lower():
#                   temp_list4.append(i)
#               if not pd.isna(row['típus']):
#                 if i==row['típus'].strip().lower():
#                   temp_list4.append(i)
#               if not pd.isna(row['gyártó']):
#                 if i==row['gyártó'].strip().lower():
#                   temp_list4.append(i)
#             if len(temp_list4)==0:
#               finalization_list.append([row['termék'], row['típus'], row['gyártó'], row['márka']])
        
# ###############################################################################################################################
# # IN THE finalization_list WE CAN HAVE MANY REDUNDANT LIST AS WE WERE ITERATING THROUGH THE list_extraction AND DID THE CHECK
# # FOR EACH ELEMENT BELONGING TO THE SAME PRODUCT LINE. SO WE HAVE TO DETECT THEM AND REMOVE THE IDENTICAL LISTS OR ELEMENTS
# ###############################################################################################################################
# #---------------------------------------
# # SMALLER THIRD PART OF THE BOTTLENECK -
# #---------------------------------------

#         # Iterate through the finalization_list
#       for sublist in finalization_list:
#           # Iterate through each element in the sublist
#           for i in range(len(sublist)):
#               # Convert each element to lowercase
#               sublist[i] = sublist[i].lower()


#         # Convert each sublist into a tuple
#       tuple_list = [tuple(sublist) for sublist in finalization_list]




#       # Convert the list of tuples into a set to remove duplicates
#       unique_elements_set = set(tuple_list)

#       tuple_list = list(unique_elements_set)

#       # Initialize a list to store tuples that should be discarded
#       to_discard = []

#       # Iterate through each tuple
#       for i in range(len(tuple_list)):
#           # Check if the current tuple is a subset of any other tuple
#           for j in range(len(tuple_list)):
#               if i != j and all(item in tuple_list[j] for item in tuple_list[i]):
#                   to_discard.append(tuple_list[i])
#                   break  # Once a match is found, no need to continue checking

#       # Remove the tuples that should be discarded from the original set
#       result = unique_elements_set - set(to_discard)


#       result = [list(item) for item in result]



#       has_length_one = any(len(sublist) == 1 for sublist in result)

#       if has_length_one:

#         checking=[]
#         tocheck=[]
#         elements_to_delete=[]
#         for i in result:
#           if len(i)==1:
#             checking.append(i)
#           else:
#             tocheck.append(i)

#         index_=[]
#         for i in tocheck:
#           for p in i:
#             for index, row in df.iterrows():
#               if p==row['termék'].strip().lower() or p==row['típus'].strip().lower() or p==row['gyártó'].strip().lower() or p==row['márka'].strip().lower():
#                 index_.append(index)

#         element_counts = Counter(index_)
#         duplicates = [element for element, count in element_counts.items() if count >1]


#         for k in checking:
#           a=True
#           for i in duplicates:
#             if a==True:
#               lowercased_arr = np.array([str(item).lower() for item in df.iloc[i].values], dtype=object)
#               for i in lowercased_arr:
#                 if k[0] in i:
#                   result.remove([k[0]])
#                   a=False
#                   break

#       topic_list.append(result)

# ###################################################################################################################
# #                    ------------------- END OF THE BOTTLENECK ------------------------                           #
# #                                                                                                                 #
# ###################################################################################################################    
   

#     topic_list2=[]
#     for firstlist_position in range(len(topic_list)):
#       elements_to_delete=[]
#       for secondlist_position in range(len(topic_list[firstlist_position])):
#         for secondlist_position2 in range(len(topic_list[firstlist_position])):
#           if len(topic_list[firstlist_position][secondlist_position])>2:
#               if len(topic_list[firstlist_position][secondlist_position2])>2 and topic_list[firstlist_position][secondlist_position]!=topic_list[firstlist_position][secondlist_position2]:
#                 if  (topic_list[firstlist_position][secondlist_position][2]==topic_list[firstlist_position][secondlist_position2][2] and
#                 topic_list[firstlist_position][secondlist_position][1] =='típust nem említett' and topic_list[firstlist_position][secondlist_position][0] =='terméket nem említett'):
#                   elements_to_delete.append(topic_list[firstlist_position][secondlist_position])
#                 if (topic_list[firstlist_position][secondlist_position][2]==topic_list[firstlist_position][secondlist_position2][2] and
#                 topic_list[firstlist_position][secondlist_position2][1] =='típust nem említett' and topic_list[firstlist_position][secondlist_position2][0] =='terméket nem említett'):
#                   elements_to_delete.append(topic_list[firstlist_position][secondlist_position2])

#                 if (topic_list[firstlist_position][secondlist_position][2]==topic_list[firstlist_position][secondlist_position2][2] and
#                 topic_list[firstlist_position][secondlist_position][1] =='típust nem említett' and topic_list[firstlist_position][secondlist_position][0]==topic_list[firstlist_position][secondlist_position2][0]):
#                   elements_to_delete.append(topic_list[firstlist_position][secondlist_position])
#                 if (topic_list[firstlist_position][secondlist_position][2]==topic_list[firstlist_position][secondlist_position2][2] and
#                 topic_list[firstlist_position][secondlist_position2][1] =='típust nem említett' and topic_list[firstlist_position][secondlist_position][0]==topic_list[firstlist_position][secondlist_position2][0]):
#                   elements_to_delete.append(topic_list[firstlist_position][secondlist_position2])
    
      
#       elements_to_delete = [list(x) for x in set(tuple(x) for x in elements_to_delete)]
#       topic_list2.append([x for x in topic_list[firstlist_position] if x not in elements_to_delete])



#     # Add the list as a new column named 'topic' to the DataFrame
#     df_pandas['topic'] = topic_list2

# #    DIFFERNETIATE BETWEEN 4,3,2,1 ITEMED LISTS IN THE THE TOPIC


#     list_for_4items=[]
#     for index, row in df_pandas.iterrows():
#       for p in row['topic']:
#         if len(p)==4:
#          list_for_4items.append(p)

#     grouped_dict = {}

#     for sublist in list_for_4items:
#         key = sublist[0]
#         if key in grouped_dict.keys():
#             grouped_dict[key].append(sublist)
#         else:
#             grouped_dict[key] = [sublist]


#     # Convert the dictionary values to a list
#     grouped_list = list(grouped_dict.values())

#     collection = []
#     for x in grouped_list:
#         summary = [defaultdict(int) for _ in range(len(grouped_list[0][0]))]
#         for sublist in x:
#             for i, item in enumerate(sublist):
#                 summary[i][item] += 1

#         final_result = []
#         for s in summary:
#             result = []
#             for key, value in s.items():
#                 result.append({key: value})
#             final_result.append(result)

#         collection.append(final_result)
    
#     list_for_3items=[]
#     for index, row in df_pandas.iterrows():
#       for p in row['topic']:
#         if len(p)==3:
#           list_for_3items.append(p)


#     list_for_2items=[]
#     for index, row in df_pandas.iterrows():
#       for p in row['topic']:
#         if len(p)==2:
#           list_for_2items.append(p)

#     list_for_1items=[]
#     for index, row in df_pandas.iterrows():
#       for p in row['topic']:
#         if len(p)==1:
#           list_for_1items.append(p)

#     #####################################################################
#     #          MANAGING 3 ITEM LIST FOR THE data_to_transform LIST      #
#     #####################################################################

#     new_3_items=[]


#     # y:[['zongora', 'akusztikus zongora', 'yamaha'],['zongora', 'akusztikus zongora', 'petrof']]

#     for p in list_for_3items:  # MÉG NESTED LISTÁVAL CSINÁLOM, LEHETNE NÉLKÜLE
#       # for p in y:
#         # IDENTIFY NEW 3 ITEM PRODUCT LINE, nincs benne az eddigi 4 itemes listába
#       q=[]
#       for k in range(len(collection)):
#         if p[0] not in collection[k][0][0].keys():
#           q.append("no")
#         else:
#           q.append("yes")
#             # INCREMENT EXISTING DICTIONARIES IN THE COLLECTION LIST 1.ROOT ITEMS
#           collection[k][0][0][list(collection[k][0][0].keys())[0]]+=1
#           # INCREMENT EXISTING DICTIONARIES IN THE COLLECTION LIST 2. ITEMS
#           temp=[]
#           for counter2 in range(len(collection[k][1])):
#             if p[1] in collection[k][1][counter2].keys():
#               collection[k][1][counter2][list(collection[k][1][counter2].keys())[0]]+=1

#           # IDENTIFY AND ADD NEW DICTIONARY IN THE COLLECTION LIST 2. ITEMS
#               temp.append("yes")
#             else:
#               temp.append("no")
#           if all(element == 'no' for element in temp):
#             collection[k][1].append({p[1]:1})

#           # INCREMENT EXISTING DICTIONARIES IN THE COLLECTION LIST 3. ITEMS
#           temp=[]
#           for counter2 in range(len(collection[k][2])):
#             if p[2] in collection[k][2][counter2].keys():
#               collection[k][2][counter2][list(collection[k][2][counter2].keys())[0]]+=1

#           # IDENTIFY AND ADD NEW DICTIONARY IN THE COLLECTION LIST 3. ITEMS
#               temp.append("yes")
#             else:
#               temp.append("no")
#           if all(element == 'no' for element in temp):
#             collection[k][2].append({p[2]:1})



#     # ------------- Adding the new 3 item collection to the main collection

#       if all(element == 'no' for element in q):
#         new_3_items.append(p)

#     grouped_dict_new_3_items = {}

#     for sublist in new_3_items:
#         key = sublist[0]
#         if key in grouped_dict_new_3_items.keys():
#             grouped_dict_new_3_items[key].append(sublist)
#         else:
#             grouped_dict_new_3_items[key] = [sublist]


#     # Convert the dictionary values to a list
#     grouped_list_new_3_item = list(grouped_dict_new_3_items.values())

#     collection_new_3_item = []


#     for x in grouped_list_new_3_item:
#         summary = [defaultdict(int) for _ in range(len(grouped_list_new_3_item[0][0]))]
#         for sublist in x:
#             for i, item in enumerate(sublist):
#                 summary[i][item] += 1

#         final_result = []
#         for s in summary:
#             result = []
#             for key, value in s.items():
#                 result.append({key: value})
#             final_result.append(result)

#         collection_new_3_item.append(final_result)

#     for i in collection_new_3_item:
#       collection.append(i)

#     ######################################
#     #         TWO ITEMED LIST            #
#     ######################################
#     new_2_items=[]   # [['gitár', 'elektromos'], ['effektpedál', 'effektpedál']]
#     for p in list_for_2items:  # MÉG NESTED LISTÁVAL CSINÁLOM, LEHETNE NÉLKÜLE
#       # for p in y:
#         # IDENTIFY NEW 3 ITEM PRODUCT LINE, nincs benne az eddigi 4 itemes listába
#       q=[]
#       for k in range(len(collection)):
#         if p[0] not in collection[k][0][0].keys():
#           q.append("no")
#         else:
#           q.append("yes")
#             # INCREMENT EXISTING DICTIONARIES IN THE COLLECTION LIST 1.ROOT ITEMS
#           collection[k][0][0][list(collection[k][0][0].keys())[0]]+=1
#           # INCREMENT EXISTING DICTIONARIES IN THE COLLECTION LIST 2. ITEMS
#           temp=[]
#           for counter2 in range(len(collection[k][1])):
#             if p[1] in collection[k][1][counter2].keys():
#               collection[k][1][counter2][list(collection[k][1][counter2].keys())[0]]+=1

#           # IDENTIFY AND ADD NEW DICTIONARY IN THE COLLECTION LIST 2. ITEMS
#               temp.append("yes")
#             else:
#               temp.append("no")
#           if all(element == 'no' for element in temp):
#             collection[k][1].append({p[1]:1})
    
#     # ------------- Adding the new 2 item collection to the main collection

#       if all(element == 'no' for element in q):
#         new_2_items.append(p)

#     grouped_dict_new_2_items = {}

#     for sublist in new_2_items:
#         key = sublist[0]
#         if key in grouped_dict_new_2_items.keys():
#             grouped_dict_new_2_items[key].append(sublist)
#         else:
#             grouped_dict_new_2_items[key] = [sublist]


#     # Convert the dictionary values to a list
#     grouped_list_new_2_item = list(grouped_dict_new_2_items.values())

#     collection_new_2_item = []


#     for x in grouped_list_new_2_item:
#         summary = [defaultdict(int) for _ in range(len(grouped_list_new_2_item[0][0]))]
#         for sublist in x:
#             for i, item in enumerate(sublist):
#                 summary[i][item] += 1

#         final_result = []
#         for s in summary:
#             result = []
#             for key, value in s.items():
#                 result.append({key: value})
#             final_result.append(result)

#         collection_new_2_item.append(final_result)

#     for i in collection_new_2_item:
#       collection.append(i)



#     ######################################
#     #         ONE ITEM LIST            #
#     ######################################
#     new_1_items=[]
#     # y: [['erősítő'], ['erősítő']]
#     for p in list_for_1items:  # MÉG NESTED LISTÁVAL CSINÁLOM, LEHETNE NÉLKÜLE
#       # for p in y:
#         # IDENTIFY NEW 3 ITEM PRODUCT LINE, nincs benne az eddigi 4 itemes listába
#       q=[]
#       for k in range(len(collection)):
#         if p[0] not in collection[k][0][0].keys():
#           q.append("no")
#         else:
#           q.append("yes")
#             # INCREMENT EXISTING DICTIONARIES IN THE COLLECTION LIST 1.ROOT ITEMS
#           collection[k][0][0][list(collection[k][0][0].keys())[0]]+=1


#       if all(element == 'no' for element in q):
#         new_1_items.append(p)

#     grouped_dict_new_1_items = {}


#     for sublist in new_1_items:
#         key = sublist[0]
#         if key in grouped_dict_new_1_items.keys():
#             grouped_dict_new_1_items[key].append(sublist)
#         else:
#             grouped_dict_new_1_items[key] = [sublist]

#     # Convert the dictionary values to a list
#     grouped_list_new_1_item = list(grouped_dict_new_1_items.values())

#     collection_new_1_item = []


#     for x in grouped_list_new_1_item:
#         summary = [defaultdict(int) for _ in range(len(grouped_list_new_1_item[0][0]))]
#         for sublist in x:
#             for i, item in enumerate(sublist):
#                 summary[i][item] += 1

#         final_result = []
#         for s in summary:
#             result = []
#             for key, value in s.items():
#                 result.append({key: value})
#             final_result.append(result)

#         collection_new_1_item.append(final_result)

#     for i in collection_new_1_item:
#       collection.append(i)

#     data_to_transform.append(collection)
#     # if len(sub_dataframes)>1:
#     #   data_to_transform.append(collection)
#     # else:
#     #   data_to_transform=collection

#   # print(data_to_transform)

#   #######################################################################################################################################################
#   #  CREATING THE final_transformed_data FOR THE CHART.JS PAGE (WE SHOULD HANDLE ONE AND MORE PERIOD LISTS DIFFERENTLY BASED ON THE DEPTH OF THE LISTS) #
#   #######################################################################################################################################################
  
#   c = ['gyártó', 'márka']

#   def calculate_depth(lst):
#       if isinstance(lst, list):
#           if lst:
#               return 1 + max(calculate_depth(item) for item in lst)
#           else:
#               return 1
#       else:
#           return 0


#   depth_of_data=calculate_depth(data_to_transform)
#   final_transformed_data = []
#   if depth_of_data==3:
#     for item in data_to_transform:
#       product_data = {}
#       product_data['label'] = list(item[0][0].keys())[0]
#       main_chart_data = [{'x': timestamp[timestamp_index], 'y': list(entry.values())[0]} for entry in item[0]]
#       product_data['mainChartData'] = main_chart_data
#       if len(item[0])==0:
#         product_data="There was no chat activity in this period"
#       if len(item)>1:
#         secondaryChartData = [list(entry.values())[0] for entry in item[1]]
#         product_data['x_secondary'] = [key for entry in item[1] for key in entry.keys()]
#         product_data['secondaryChartData'] = [secondaryChartData]
#       if len(item)>2:
#         secondaryChartData_b = [list(entry.values())[0] for entry in item[2]]
#         product_data['label_b'] = c[0]
#         product_data['x_secondary_b'] = [key for entry in item[2] for key in entry.keys()]
#         product_data['secondaryChartData_b'] = [secondaryChartData_b]
#       if len(item)>3:
#         secondaryChartData_c = [list(entry.values())[0] for entry in item[3]]
#         product_data['label_c'] = c[1]
#         product_data['x_secondary_c'] = [key for entry in item[3] for key in entry.keys()]
#         product_data['secondaryChartData_c'] = [secondaryChartData_c]

#       if len(item)==1:
#         secondaryChartData = [list(entry.values())[0] for entry in item[0]]
#         product_data['x_secondary'] = ["Típusról nem folyt beszélgetés"]
#         product_data['secondaryChartData'] = [secondaryChartData]
#         secondaryChartData_b = [list(entry.values())[0] for entry in item[0]]
#         product_data['label_b'] = c[0]
#         product_data['x_secondary_b'] = ["Gyártóról nem folyt beszélgetés"]
#         product_data['secondaryChartData_b'] = [secondaryChartData_b]
#         secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
#         product_data['label_c'] = c[1]
#         product_data['x_secondary_c'] = ["Márkatípusról nem folyt beszélgetés"]
#         product_data['secondaryChartData_c'] = [secondaryChartData_c]
#       if len(item)==2:
#         secondaryChartData_b = [list(entry.values())[0] for entry in item[0]]
#         product_data['label_b'] = c[0]
#         product_data['x_secondary_b'] = ["Gyártóról nem folyt beszélgetés"]
#         product_data['secondaryChartData_b'] = [secondaryChartData_b]
#         secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
#         product_data['label_c'] = c[1]
#         product_data['x_secondary_c'] = ["Márkatípusról nem folyt beszélgetés"]
#         product_data['secondaryChartData_c'] = [secondaryChartData_c]
#       if len(item)==3:
#         secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
#         product_data['label_c'] = c[1]
#         product_data['x_secondary_c'] = ["Márkatípusról nem folyt beszélgetés"]
#         product_data['secondaryChartData_c'] = [secondaryChartData_c]
#       final_transformed_data.append(product_data)

#   def longest_mainChartData(consolidated_list):
#     max_length = 0
#     max_item = None

#     # Iterate over each dictionary in the list
#     for item in consolidated_list:
#         # Get the length of the mainChartData list
#         length = len(item.get('mainChartData', []))

#         # Check if the length is longer than the current maximum
#         if length > max_length:
#             max_length = length
#             max_item = item
#     return max_length, max_item

#   # Initialize a new list to store the consolidated items
#   data_for_final_transformation=[]  
#   consolidated_list = []

#   if depth_of_data==4:
#     timestamp_index=0
#     for period in data_to_transform:
#       period_to_add_to_finaltransformation=[]
#       for item in period:
#         product_data = {}
#         product_data['label'] = list(item[0][0].keys())[0]
#         main_chart_data = [{'x': timestamp[timestamp_index], 'y': list(entry.values())[0]} for entry in item[0]]
#         product_data['mainChartData'] = main_chart_data
#         if len(item)>1:
#           secondaryChartData = [list(entry.values())[0] for entry in item[1]]
#           product_data['x_secondary'] = [key for entry in item[1] for key in entry.keys()]
#           product_data['secondaryChartData'] = [secondaryChartData]
#         if len(item)>2:
#           secondaryChartData_b = [list(entry.values())[0] for entry in item[2]]
#           product_data['label_b'] = c[0]
#           product_data['x_secondary_b'] = [key for entry in item[2] for key in entry.keys()]
#           product_data['secondaryChartData_b'] = [secondaryChartData_b]
#         if len(item)>3:
#           secondaryChartData_c = [list(entry.values())[0] for entry in item[3]]
#           product_data['label_c'] = c[1]
#           product_data['x_secondary_c'] = [key for entry in item[3] for key in entry.keys()]
#           product_data['secondaryChartData_c'] = [secondaryChartData_c]

#         if len(item)==1:
#           secondaryChartData = [list(entry.values())[0] for entry in item[0]]
#           product_data['x_secondary'] = ["Típusról nem folyt beszélgetés"]
#           product_data['secondaryChartData'] = [secondaryChartData]
#           secondaryChartData_b = [list(entry.values())[0] for entry in item[0]]
#           product_data['label_b'] = c[0]
#           product_data['x_secondary_b'] = ["Gyártóról nem folyt beszélgetés"]
#           product_data['secondaryChartData_b'] = [secondaryChartData_b]
#           secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
#           product_data['label_c'] = c[1]
#           product_data['x_secondary_c'] = ["Márkatípusról nem folyt beszélgetés"]
#           product_data['secondaryChartData_c'] = [secondaryChartData_c]
#         if len(item)==2:
#           secondaryChartData_b = [list(entry.values())[0] for entry in item[0]]
#           product_data['label_b'] = c[0]
#           product_data['x_secondary_b'] = ["Gyártóról nem folyt beszélgetés"]
#           product_data['secondaryChartData_b'] = [secondaryChartData_b]
#           secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
#           product_data['label_c'] = c[1]
#           product_data['x_secondary_c'] = ["Márkatípusról nem folyt beszélgetés"]
#           product_data['secondaryChartData_c'] = [secondaryChartData_c]
#         if len(item)==3:
#           secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
#           product_data['label_c'] = c[1]
#           product_data['x_secondary_c'] = ["Márkatípusról nem folyt beszélgetés"]
#           product_data['secondaryChartData_c'] = [secondaryChartData_c]
#         period_to_add_to_finaltransformation.append(product_data)
    
#       timestamp_index+=1
#       data_for_final_transformation.append(period_to_add_to_finaltransformation)
    
#     # for main_itemxx in data_for_final_transformation:
#     #   print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
#     #   print(main_itemxx)
#     #   print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    
    
#     for main_item in data_for_final_transformation:
#       if len(consolidated_list)==0:
#         consolidated_list=main_item
#         continue

#       #HANDELING THOSE ITEMS WHICH HAS COMMON LABEL IN DIFFERENT PERIODS

#       for item_a in main_item:
      
#       # Check if the item's label exists in list b
#         if any(item_a['label'] == item_b['label'] for item_b in consolidated_list):
#           # Find the corresponding item in list b
#           item_b = next(item_b for item_b in consolidated_list if item_b['label'] == item_a['label'])

#           # Create a new item to store the consolidated data
#           consolidated_item = {'label': item_a['label']}
#           b_time=item_b['mainChartData'][-1]['x']
#           a_time=item_a['mainChartData'][0]['x']
#           b_time = datetime.strptime(b_time, "%Y-%m-%d %H:%M:%S")
#           a_time = datetime.strptime(a_time, "%Y-%m-%d %H:%M:%S")

#           # Consolidate mainChartData
    
#           # if a_time < b_time:
#           #     consolidated_item['mainChartData'] = item_a['mainChartData'] + item_b['mainChartData']
        
          
#           if a_time > b_time:
#             consolidated_item['mainChartData'] = item_b['mainChartData'] + item_a['mainChartData']

#           # Consolidate x_secondary
#           consolidated_item['x_secondary'] = list(set(item_a['x_secondary'] + item_b['x_secondary']))

#           # Consolidate secondaryChartData
#           result_a = [0] * len(consolidated_item['x_secondary'])
#           for index_a,value_a in enumerate(item_a['x_secondary']):
#             for index_cons, value_cons in enumerate(consolidated_item['x_secondary']):
#               if value_a ==value_cons:
#                 result_a[index_cons] = item_a['secondaryChartData'][0][index_a]
#             result_b=[]
#             for x in range(len(item_b['mainChartData'])):
#               result_init=[0] * len(consolidated_item['x_secondary'])
#               for index_b,value_b in enumerate(item_b['x_secondary']):
#                 for index_cons, value_cons in enumerate(consolidated_item['x_secondary']):
#                   if value_b ==value_cons:
#                     result_init[index_cons] = item_b['secondaryChartData'][x][index_b]
#               result_b.append(result_init)
#             if a_time < b_time:
#               consolidated_item['secondaryChartData'] = [result_a]+ result_b
#             elif a_time > b_time:
#               consolidated_item['secondaryChartData'] = result_b+ [result_a]


#           # Consolidate label_b x_secondary_b and Consolidate secondaryChartData_b
#           consolidated_item['label_b'] = 'gyártó'
#           if 'x_secondary_b' not in item_a and 'x_secondary_b' not in item_b:
#             consolidated_item['x_secondary_b']=['Gyártóval kapcsolatban nem történt beszélgetés']
#             result_b=[]
#             for x in item_b['mainChartData']:
#               result_init=[0]
#               result_b.append(result_init)
#             consolidated_item['secondaryChartData_b'] = result_b + [[0]]



#           if 'x_secondary_b' in item_a and 'x_secondary_b' not in item_b:
#             consolidated_item['x_secondary_b']=item_a['x_secondary_b']
#             result_b=[]
#             for x in item_b['mainChartData']:
#               result_init=[0] * len(consolidated_item['x_secondary_b'])
#               result_b.append(result_init)
#             if a_time < b_time:
#               consolidated_item['secondaryChartData_b'] = item_a['secondaryChartData_b']+ result_b
#             elif a_time > b_time:
#               consolidated_item['secondaryChartData_b'] = result_b+ item_a['secondaryChartData_b']

#           if 'x_secondary_b' in item_b and 'x_secondary_b' not in item_a:
#             consolidated_item['x_secondary_b']=item_b['x_secondary_b']
#             if a_time < b_time:
#               consolidated_item['secondaryChartData_b'] = item_a['secondaryChartData_b']+ [[0]]
#             elif a_time > b_time:
#               consolidated_item['secondaryChartData_b'] = [[0]]+ item_a['secondaryChartData_b']

#           if 'x_secondary_b' in item_b and 'x_secondary_b'  in item_a:
#             consolidated_item['x_secondary_b'] = list(set(item_a['x_secondary_b'] + item_b['x_secondary_b']))
#             result_a = [0] * len(consolidated_item['x_secondary_b'])
#             for index_a,value_a in enumerate(item_a['x_secondary_b']):
#               for index_cons, value_cons in enumerate(consolidated_item['x_secondary_b']):
#                 if value_a ==value_cons:
#                   result_a[index_cons] = item_a['secondaryChartData_b'][0][index_a]
#               result_b=[]
#               for x in range(len(item_b['mainChartData'])):
#                 result_init=[0] * len(consolidated_item['x_secondary_b'])
#                 for index_b,value_b in enumerate(item_b['x_secondary_b']):
#                   for index_cons, value_cons in enumerate(consolidated_item['x_secondary_b']):
#                     if value_b ==value_cons:
#                       result_init[index_cons] = item_b['secondaryChartData_b'][x][index_b]
#                 result_b.append(result_init)
#               if a_time < b_time:
#                 consolidated_item['secondaryChartData_b'] = [result_a]+ result_b
#               elif a_time > b_time:
#                 consolidated_item['secondaryChartData_b'] = result_b+ [result_a]

#           # Consolidate label_c  x_secondary_c secondaryChartData_c
#           consolidated_item['label_c'] = 'márka'
#           if 'x_secondary_c' not in item_a and 'x_secondary_c' not in item_b:
#             consolidated_item['x_secondary_c']=['Márkával kapcsolatban nem történt beszélgetés']
#             result_b=[]
#             for x in item_b['mainChartData']:
#               result_init=[0]
#               result_b.append(result_init)
#             consolidated_item['secondaryChartData_c'] = result_b + [[0]]



#           if 'x_secondary_c' in item_a and 'x_secondary_c' not in item_b:
#             consolidated_item['x_secondary_c']=item_a['x_secondary_c']
#             result_b=[]
#             for x in item_b['mainChartData']:
#               result_init=[0] * len(consolidated_item['x_secondary_c'])
#               result_b.append(result_init)
#             if a_time < b_time:
#               consolidated_item['secondaryChartData_c'] = item_a['secondaryChartData_c']+ result_b
#             elif a_time > b_time:
#               consolidated_item['secondaryChartData_c'] = result_b+ item_a['secondaryChartData_c']

#           if 'x_secondary_c' in item_b and 'x_secondary_c' not in item_a:
#             consolidated_item['x_secondary_c']=item_b['x_secondary_c']
#             if a_time < b_time:
#               consolidated_item['secondaryChartData_c'] = item_a['secondaryChartData_c']+ [[0]]
#             elif a_time > b_time:
#               consolidated_item['secondaryChartData_c'] = [[0]]+ item_a['secondaryChartData_c']


#           if 'x_secondary_c' in item_b and 'x_secondary_c'  in item_a:
#             consolidated_item['x_secondary_c'] = list(set(item_a['x_secondary_c'] + item_b['x_secondary_c']))
#             result_a = [0] * len(consolidated_item['x_secondary_c'])
#             for index_a,value_a in enumerate(item_a['x_secondary_c']):
#               for index_cons, value_cons in enumerate(consolidated_item['x_secondary_c']):
#                 if value_a ==value_cons:
#                   result_a[index_cons] = item_a['secondaryChartData_c'][0][index_a]
#               result_b=[]
#               for x in range(len(item_b['mainChartData'])):
#                 result_init=[0] * len(consolidated_item['x_secondary_c'])
#                 for index_b,value_b in enumerate(item_b['x_secondary_c']):
#                   for index_cons, value_cons in enumerate(consolidated_item['x_secondary_c']):
#                     if value_b ==value_cons:
#                       result_init[index_cons] = item_b['secondaryChartData_c'][x][index_b]
#                 result_b.append(result_init)
#               if a_time < b_time:
#                 consolidated_item['secondaryChartData_c'] = [result_a]+ result_b
#               elif a_time > b_time:
#                 consolidated_item['secondaryChartData_c'] = result_b+ [result_a]


#           for index, item in enumerate(consolidated_list):
#                 if item == item_b:
                
#                     consolidated_list [index] = consolidated_item
            
        
                

#     #HANDELING THOSE ITEMS WHICH DON'T HAVE COMMON LABEL IN DIFFERENT PERIODS

#         # Determine all unique label values in lists a and b
#         all_labels = set(sublist['label'] for sublist in consolidated_list)
        
#         # # Initialize a list to store the labels that can be consolidated
#         # consolidatable_labels = []

#         # # Iterate over each label value
#         # for label in all_labels:
#         #     # Check if the label value exists in both lists a and b
#         #     if any(item['label'] == label for item in main_item):
#         #         consolidatable_labels.append(label)
        

    

#         # Iterate over the unconsolidated lists
      
        
#         if item_a['label'] not in all_labels:

#           # Complete the mainChartData with missing months
#           max_length, max_item=longest_mainChartData(consolidated_list)
        
#           consolidated_item = {'label': item_a['label']}

#           b_time= max_item['mainChartData'][-1]['x']
#           a_time=item_a['mainChartData'][0]['x']
#           b_time = datetime.strptime(b_time, "%Y-%m-%d %H:%M:%S")
#           a_time = datetime.strptime(a_time, "%Y-%m-%d %H:%M:%S")

#           # Consolidate mainChartData
#           if a_time > b_time:
#             consolidated_item = {'label': item_a['label']}  
#             max_length, max_item=longest_mainChartData(consolidated_list)
#             chartdata=[]
#             for r in range(len(max_item['mainChartData'])):
#               x=max_item['mainChartData'][r]['x']
#               chartdata.append({'x': x, 'y': 0})
#             chartdata.append(item_a['mainChartData'][0])
#             consolidated_item['mainChartData']=chartdata
          
          
            


#             consolidated_item['x_secondary']=item_a['x_secondary']
#             result_b=[]
#             consolidated_item['label_b'] = 'gyártó'
#             for x in max_item['mainChartData']:
#               result_init=[0] * len(item_a['x_secondary'])
#               result_b.append(result_init)
#             consolidated_item['secondaryChartData'] = result_b + item_a['secondaryChartData']
#             if 'x_secondary_b' in item_a:
#               consolidated_item['x_secondary_b']=item_a['x_secondary_b']
#               result_b=[]
#               for x in  max_item['mainChartData']:
#                 result_init=[0] * len(item_a['x_secondary_b'])
#                 result_b.append(result_init)
#               consolidated_item['secondaryChartData_b'] = result_b + item_a['secondaryChartData_b']
#             if 'x_secondary_b' not in item_a:
#               consolidated_item['x_secondary_b']=['Gyártóval kapcsolatban nem történt beszélgetés']
#               result_b=[]
#               for x in item_b['mainChartData']:
#                 result_init=[0]
#                 result_b.append(result_init)
#               consolidated_item['secondaryChartData_b'] = result_b + [[0]]
#             consolidated_item['label_c'] = 'márka'
#             if 'x_secondary_c' in item_a:
#               consolidated_item['x_secondary_c']=item_a['x_secondary_c']
#               result_b=[]
#               for x in max_item['mainChartData']:
#                 result_init=[0] * len(item_a['x_secondary_c'])
#                 result_b.append(result_init)
#               consolidated_item['secondaryChartData_c'] = result_b + item_a['secondaryChartData_c']
#             if 'x_secondary_c' not in item_a:
#               consolidated_item['x_secondary_c']=['Márkával kapcsolatban nem történt beszélgetés']
#               result_b=[]
#               for x in item_b['mainChartData']:
#                 result_init=[0]
#                 result_b.append(result_init)
#               consolidated_item['secondaryChartData_c'] = result_b + [[0]]
          

#           if a_time < b_time:
            
#             consolidated_item = {'label': item_a['label']}
#             max_length, max_item=longest_mainChartData(consolidated_list)
#             consolidated_item['mainChartData'] = max_item['mainChartData'] + item_a['mainChartData']
#             consolidated_item['x_secondary']=item_a['x_secondary']
#             consolidated_item['label_b'] = 'gyártó'
#             result_b=[]
#             for x in  max_item['mainChartData']:
#               result_init=[0] * len(item_a['x_secondary'])
#               result_b.append(result_init)
#             consolidated_item['secondaryChartData'] = item_a['secondaryChartData'] +  result_b
#             if 'x_secondary_b' in item_a:
#               consolidated_item['x_secondary_b']=item_a['x_secondary_b']
#               result_b=[]
#               for x in  max_item['mainChartData']:
#                 result_init=[0] * len(item_a['x_secondary_b'])
#                 result_b.append(result_init)
#               consolidated_item['secondaryChartData_b'] = item_a['secondaryChartData_b']+result_b
#               if 'x_secondary_b' not in item_a:
#                 consolidated_item['x_secondary_b']=['Gyártóval kapcsolatban nem történt beszélgetés']
#               result_b=[]
#               for x in item_b['mainChartData']:
#                 result_init=[0]
#                 result_b.append(result_init)
#               consolidated_item['secondaryChartData_b'] =  [[0]] + result_b
#             consolidated_item['label_c'] = 'márka'
#             if 'x_secondary_c' in item_a:
#               consolidated_item['x_secondary_c']=item_a['x_secondary_c']
#               result_b=[]
#               for x in  max_item['mainChartData']:
#                 result_init=[0] * len(item_a['x_secondary_c'])
#                 result_b.append(result_init)
#               consolidated_item['secondaryChartData_c'] =  item_a['secondaryChartData_c'] + result_b
#             if 'x_secondary_c' not in item_a:
#               consolidated_item['x_secondary_c']=['Márkával kapcsolatban nem történt beszélgetés']
#               result_b=[]
#               for x in item_b['mainChartData']:
#                 result_init=[0]
#                 result_b.append(result_init)
#               consolidated_item['secondaryChartData_c'] =  [[0]] + result_b

#           if a_time == b_time:
#             consolidated_item = {'label': item_a['label']}
#             max_length, max_item=longest_mainChartData(consolidated_list)

#             chartdata=[]
#             for r in range(len(max_item['mainChartData'])-1):
#               x=max_item['mainChartData'][r]['x']
#               chartdata.append({'x': x, 'y': 0})
#             chartdata.append(item_a['mainChartData'][0])
#             consolidated_item['mainChartData']=chartdata
        
          

      
#             consolidated_item['x_secondary']=item_a['x_secondary']
#             result_b=[]
#             consolidated_item['label_b'] = 'gyártó'
#             for x in  range(len(max_item['mainChartData'][:-1])):
#               result_init=[0] * len(item_a['x_secondary'])
#               result_b.append(result_init)
#             consolidated_item['secondaryChartData'] = result_b + item_a['secondaryChartData']
#             if 'x_secondary_b' in item_a:
#               consolidated_item['x_secondary_b']=item_a['x_secondary_b']
#               result_b=[]
#               for x in  range(len(max_item['mainChartData'][:-1])):
#                 result_init=[0] * len(item_a['x_secondary_b'])
#                 result_b.append(result_init)
#               consolidated_item['secondaryChartData_b'] = result_b + item_a['secondaryChartData_b']
#             if 'x_secondary_b' not in item_a:
#               consolidated_item['x_secondary_b']=['Gyártóval kapcsolatban nem történt beszélgetés']
#               result_b=[]
#               for x in  range(len(max_item['mainChartData'][:-1])):
#                 result_init=[0]
#                 result_b.append(result_init)
#               consolidated_item['secondaryChartData_b'] = result_b + [[0]]
#             consolidated_item['label_c'] = 'márka'
#             if 'x_secondary_c' in item_a:
#               consolidated_item['x_secondary_c']=item_a['x_secondary_c']
#               result_b=[]
#               for x in  range(len(max_item['mainChartData'][:-1])):
#                 result_init=[0] * len(item_a['x_secondary_c'])
#                 result_b.append(result_init)
#               consolidated_item['secondaryChartData_c'] = result_b + item_a['secondaryChartData_c']
#             if 'x_secondary_c' not in item_a:
#               consolidated_item['x_secondary_c']=['Márkával kapcsolatban nem történt beszélgetés']
#               result_b=[]
#               for x in  range(len(max_item['mainChartData'][:-1])):
#                 result_init=[0]
#                 result_b.append(result_init)
#               consolidated_item['secondaryChartData_c'] = result_b + [[0]]
        
        
#           consolidated_list.append(consolidated_item)


#     # UPDATE ALL ITEMS IN CONSOLIDATED LIST IF NEEDED TO HAVE THE SAME LENGTH REGARDING MAINCHARTDATA

#       max_length, max_item=longest_mainChartData(consolidated_list)

#       for item in consolidated_list:
        
#         if len(item['mainChartData'])<max_length:

              
#           x=max_item['mainChartData'][-1]['x']
        
          
#             # Append the copied last element to item['mainChartData']
#           item['mainChartData'].append({'x': x, 'y': 0})
        
#           result_init=[0] * len(item['x_secondary'])
#           item['secondaryChartData'].append(result_init)
#           if 'x_secondary_b' in item:
#             result_init=[0] * len(item['x_secondary_b'])
#             item['secondaryChartData_b'].append(result_init)
#           if 'x_secondary_c' in item:
#             result_init=[0] * len(item['x_secondary_c'])
#             item['secondaryChartData_c'].append(result_init)
    
#     final_transformed_data=consolidated_list

#   return final_transformed_data






      






   