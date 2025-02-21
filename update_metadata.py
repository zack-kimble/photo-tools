# -*- coding: utf-8 -*-
"""
Created on Sun Oct 13 17:30:04 2019

@author: zack
"""
import os
import exiftool
import pandas as pd
import numpy as np
import datetime
import pickle

original_photo_dirs = ['D:/My Pictures/D800/','D:/My Pictures/D700/Originals','D:/My Pictures/Z7/'] 

file = 'D:/My Pictures/Z7/2019-Q3/DSC_0276.JPG'
file2 = 'D:/My Pictures/Z7/2019-Q3/DSC_0276.NEF'


ph_exif = exiftool.ExifTool(executable_="C:\\Users\\zack\\install\\exiftool.exe")

ph_exif.start()
metadata_dict = ph_exif.get_metadata(file)
metadata_keys = [key for key in metadata_dict.keys()]
metadata_vals = [val for val in metadata_dict.values()]
ts_as_id = [metadata_dict['Composite:SubSecCreateDate']]*len(metadata_vals)
filepath_val = [file]*len(metadata_vals)

ph_exif.get_metadata('D:/My Pictures/D800/2014-1H/DSC_0092.JPG')
ph_exif.get_metadata('E:/My Pictures/D800/2014-1H/DSC_0092.JPG')
ph_exif.get_metadata('G:/My Pictures/D800/2014-1H/DSC_0092.JPG')

ph_exif.get_metadata('D:/My Pictures/Z7/2019-Q3/DSC_0276.JPG')

len(ph_exif.get_metadata('G:/My Pictures/D800/Shopped/DSC_0335.JPG'))
len(ph_exif.get_metadata('E:/My Pictures/D800/Shopped/DSC_0335.JPG'))

ph_exif.get_metadata('D:/My Pictures/D800/2014-2H/DSC_0108.XMP')
x =ph_exif.get_metadata('D:/My Pictures/D800/2014-2H/DSC_0108.NEF')
[(key, value) for key, value in x.items() if 'Label' in key]

x =ph_exif.get_metadata('D:/My Pictures/Z7/2019-Q3/DSC_0276.XMP')
[(key, value) for key, value in x.items() if 'Label' in key]


#list(zip(*[items for items in metadata_dict.items()]))

[key for key in metadata_dict2 if key not in [key for key in metadata_dict.keys()]]

metadata_dict2['EXIF:JpgFromRaw']



df = pd.DataFrame.from_dict(column_dictionary)
df['metadata_type'] = df['metadata_key'].apply(lambda x: x.split(':')[0])
df['metadata_key'] = df['metadata_key'].apply(lambda x: x.split(':')[-1])
df['extract_time'] = datetime.datetime.now()

for root, dirs, files in file_list:
   for name in files:
      print(os.path.join(root, name))
   for name in dirs:
      print(os.path.join(root, name))
 filepaths = [os.path.join(root,name) for root, dirs, files in file_list for name in files]

filepaths = []
for root, dirs, files in file_list:
   for name in files:
       filepaths.append(os.path.join(root, name))
       
meta_df = pd.read_pickle('meta_df.pkl')
label_related = [x for x in meta_df.metadata_key.unique() if 'abel' in x]
meta_df[meta_df.metadata_key == 'Label'].metadata_value.value_counts()

red_labeled = meta_df.loc[np.all([meta_df.metadata_key == 'Label', meta_df.metadata_value == 'Red'],axis=0)].copy()
#look for jpg for each file with red label
red_labeled['jpg_path'] =red_labeled.filepath.apply(lambda x: x.replace(x.split('.')[-1],'JPG'))
red_labeled_collapsed = red_labeled.groupby(['time_id','jpg_path']).count().reset_index()

red_jpgs = red_labeled['jpg_path'].unique()

with open('list_of_red_jpg_paths','wb') as f:
    pickle.dump(red_jpgs,f)

