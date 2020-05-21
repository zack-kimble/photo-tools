# -*- coding: utf-8 -*-
"""
Created on Sun May 17 10:07:19 2020

@author: zack
"""

import datetime
import json
import os

import exiftool
import pandas as pd
import dask

from dask.distributed import Client



# Load pickle of df
# get all existing filepaths from df
# Get all file paths from directories
# Check all filepaths against existing ones in df
# run exiftool against each path
# convert to list of keys and vals
# add filepath as another list
# add subsec date
# add
# extend existing list
# every 2000, convert long list to df and concat with existing, dump back to pickle
# Concat operation also needs to convert new df to right format: expand key prefixes to two keys: metadata type and key, adds datatime
# what to do about jpegs, NEFs and XMP for same pic? - figure out in df after? Could merge based on subsec create date


column_dictionary = dict(time_id=[], filepath=[], metadata_key=[], metadata_value=[]) #,file_suffix=[], file_prefix=[])


def get_filepaths(directories: list, file_types: list):
    walk_list = []
    for directory in directories:
        walk = os.walk(directory)
        walk_list.extend([x for x in walk])
    filepaths = [os.path.join(root, name) for root, dirs, files in walk_list for name in files]
    filepaths = [filepath for filepath in filepaths if filepath.lower().split('.')[-1] in file_types]
    return filepaths


def add_metadata_to_df(df, column_dictionary):
    return


def dictionary_to_meta_df(column_dictionary):
    new_df = pd.DataFrame.from_dict(column_dictionary)
    if len(new_df) == 0:
        print("nothing to store")
        return
    new_df['metadata_type'] = new_df['metadata_key'].apply(lambda x: x.split(':')[0])
    new_df['metadata_key'] = new_df['metadata_key'].apply(lambda x: x.split(':')[-1])
    new_df['extract_time'] = datetime.datetime.now()
    return new_df

#TODO: find and fix jpegs that have been directly edited by LR and lost trailing 0 from subsec times (currently 2)
def get_time_id(metadata_dict, dt_keys):
    for dt_key in dt_keys:
        try:
            return metadata_dict[dt_key]
        except KeyError:
            pass


def update_meta_df(photo_directories, column_dictionary, file_types, meta_df_path, exif_tool_executable=None,
                   append=True):
    if not append:
        meta_df = init_meta_df(column_dictionary)
    else:
        meta_df = load_meta_df(meta_df_path)

    filepaths = get_filepaths(photo_directories, file_types)
    filepath_set = set(filepaths)
    meta_df_filepath_set = set(meta_df['filepath'])
    new_files = filepath_set - meta_df_filepath_set
    if new_files:
        print(f'getting metadata on {len(new_files)} new files')
        ph_exif = exiftool.ExifTool(executable_=exif_tool_executable)
        ph_exif.start()
    else:
        print("no new files")
        return
    # TODO make parallel
    for i, filepath in enumerate(new_files):
        print(f'{i}: {filepath}')
        metadata_dict = ph_exif.get_metadata(filepath)
        column_dictionary['metadata_key'].extend([key for key in metadata_dict.keys()])
        column_dictionary['metadata_value'].extend([val for val in metadata_dict.values()])
        time_id = get_time_id(metadata_dict, dt_keys)
        column_dictionary['time_id'].extend([time_id] * len(metadata_dict))
        column_dictionary['filepath'].extend([filepath] * len(metadata_dict))
        column_dictionary['file_suffix'].extend([filepath.split('.')[-1]] * len(metadata_dict))
        if (i != 0 and i % 10000 == 0):
            new_meta_df = dictionary_to_meta_df(column_dictionary)
            meta_df = pd.concat([meta_df,new_meta_df])
            meta_df.to_pickle(meta_df_path)
            for value in column_dictionary.values():
                value.clear()

    new_meta_df = dictionary_to_meta_df(column_dictionary)
    meta_df = pd.concat([meta_df, new_meta_df])
    meta_df.to_pickle(meta_df_path)
    return

#TODO: Check update time
def update_meta_df_dask_futures(photo_directories, column_dictionary, file_types, meta_df_path, exif_tool_executable=None,
                   append=True):
    if not append:
        meta_df = init_meta_df(column_dictionary)
    else:
        meta_df = load_meta_df(meta_df_path)

    filepaths = get_filepaths(photo_directories, file_types)
    filepath_set = set(filepaths)
    meta_df_filepath_set = set(meta_df['filepath'])
    new_files = list(filepath_set - meta_df_filepath_set)
    if new_files:
        print(f'getting metadata on {len(new_files)} new files')
    else:
        print("no new files")
        return

    client = Client()  # start local workers as processes

    def dask_worker_exif(args_tuple):
        column_dictionary, filepaths = args_tuple
        ph_exif = exiftool.ExifTool()
        ph_exif.start()
        for filepath in filepaths:
            metadata_dict = ph_exif.get_metadata(filepath)
            column_dictionary['metadata_key'].extend([key for key in metadata_dict.keys()])
            column_dictionary['metadata_value'].extend([val for val in metadata_dict.values()])
            time_id = get_time_id(metadata_dict, dt_keys)
            column_dictionary['time_id'].extend([time_id] * len(metadata_dict))
            column_dictionary['filepath'].extend([filepath] * len(metadata_dict))
            #column_dictionary['file_suffix'].extend([filepath.split('.')[-1]] * len(metadata_dict))
            #column_dictionary['file_prefix'].extend([filepath.split('.')[0]] * len(metadata_dict))
        return dictionary_to_meta_df(column_dictionary)


    def chunks(l, n):
        """Yield n number of striped chunks from l."""
        for i in range(0, n):
            yield l[i::n]

    args = [(column_dictionary, x) for x in chunks(new_files, 8 )]
    futures = client.map(dask_worker_exif, args)
    results = client.gather(futures)
    new_df = pd.concat(results)

    meta_df = pd.concat([meta_df, new_df])
    meta_df.to_pickle(meta_df_path)
    return meta_df

def split_filepath(meta_df):
    meta_df['file_suffix'] = meta_df['filepath'].apply(lambda x: x.split('.')[-1])
    meta_df['file_prefix'] = meta_df['filepath'].apply(lambda x: x.split('.')[0])
    return meta_df

def init_meta_df(column_dictionary):
    df = pd.DataFrame.from_dict(column_dictionary)
    df['metadata_type'] = df['metadata_key'].apply(lambda x: x.split(':')[0])
    df['metadata_key'] = df['metadata_key'].apply(lambda x: x.split(':')[-1])
    df['extract_time'] = datetime.datetime.now()
    meta_df = df.truncate(after=-1)
    return meta_df


def load_meta_df(meta_df_path):
    meta_df = pd.read_pickle('meta_df.pkl')
    return meta_df


# Multiple options for dealing with JPGs
# Given that
# 1. There are .NEF and .JPG for most, but not all images
# 2. Any tags or labels from Lightroom are applied only to .NEF
# 3. JPG contains most all other exif
# 4. .NEF contains its own embedded JPG which could be extracted and is presumably the same image as seperate file (but has no exif)
# 5. Also need .xmp data
# Options
# doesn't work for xmp 1. Filter out all jpgs, do all filtering and analysis on .NEF. Output any JPGs from the .NEF either by looking for file or extracting and adding exif
# doesn't work for xmp 2. Make seperate df for nef vs jpeg and filter for each (requires step to transfer labels and tags to JPEGs)
# 3. Create some sort of meta object that contains Union of .NEF and .JPG tags and can return either format.
#  - This will get tricky sticking to DF. Would need an image df and then an attribute df, essentially more normalized than currently.
#  - keep track of source filetype for each attribute
#   - make another df for time_id and file path without suffix
#  - make another df with merged attributes? Classify attributes as single source or shared? What about NEF and XMP but not JPG?
#       - for now the decision is to do NEF, JPG, and XMP only and then a single value for "multi"

def create_id_df(meta_df):
    id_df = meta_df[['time_id', 'file_prefix']].copy()
    id_df = id_df.drop_duplicates(subset=['time_id','file_prefix'])
    #id_df['file_prefix'] = id_df['filepath'].apply(lambda x: x.split('.')[0])
    return id_df

def create_merged_meta_df(meta_df):
    #meta_df = pd.read_pickle('meta_df.pkl')
    merged_meta_df = meta_df.copy()
    merged_meta_df['files_with_key'] = merged_meta_df.groupby(['time_id', 'metadata_key', 'metadata_type'])['filepath'].transform('count')
    merged_meta_df.loc[merged_meta_df.files_with_key >1, 'source_file_type']  = 'multi'
    merged_meta_df.loc[merged_meta_df.files_with_key ==1, 'source_file_type' ] = merged_meta_df.loc[merged_meta_df.files_with_key ==1, 'file_suffix']
    missing_time_id = merged_meta_df[merged_meta_df.source_file_type.isna()]
    merged_meta_df = (merged_meta_df.dropna(subset=['time_id'])\
        .drop_duplicates(subset=['time_id', 'metadata_key', 'metadata_type','source_file_type'])
        .drop(columns=['filepath','file_suffix'])
        .set_index(['time_id','metadata_type','metadata_key'])
        )
    return merged_meta_df


def convert_filepath_to_kws(meta_id_df, original_photo_dirs):
    relevant_path_structure = meta_id_df.set_index('time_id').file_prefix.copy()
    for directory in original_photo_dirs:
        relevant_path_structure = relevant_path_structure.str.replace(directory,'')
    new_kws = relevant_path_structure.apply(lambda x: x.split('.')[:-1])
    return new_kws

def extend_kws(series):
    current = series['metadata_value']
    if type(current) == str:
        current = [current]
    new = list(series['new_kws'])
    current.extend(new)
    return current

def add_kws_to_df(kws, meta_df):
    kws = kws.rename('new_kws')
    kw_metadata = meta_df.query("metadata_key in ['Subject','HierarchicalSubject']").copy()
    kw_metadata = kw_metadata.reset_index(['metadata_type', 'metadata_key']).join(kws)
    kw_metadata['metadata_value'] = kw_metadata.apply(extend_kws, axis=1)
    kw_metadata = kw_metadata.reset_index().set_index(['time_id','metadata_type','metadata_key'])
    meta_df.update(kw_metadata)
    return meta_df


def filter_by_metadata(filter_dictionary):
    pass

# def make_jpg_meta_df(meta_df, meta_df_path, red_label_only=True):

#     if red_label_only:
#         jpg_meta_df = meta_df.loc[np.all([meta_df.metadata_key == 'Label', meta_df.metadata_value == 'Red'],axis=0)].copy()
#     else:
#         jpg_meta_df = meta_df.copy()

#     jpg_meta_df['jpg_path'] = jpg_meta_df.filepath.apply(lambda x: x.replace(x.split('.')[-1],'JPG'))
#     jpg_meta_df = jpg_meta_df.groupby(['time_id','jpg_path']).count().reset_index()

#     red_jpgs = red_labeled['jpg_path'].unique()

#     with open('list_of_red_jpg_paths','wb') as f:
#         pickle.dump(red_jpgs,f)


if __name__ == '__main__':
    config_path = 'ubuntu_config.json'
    with open(config_path, 'r') as f:
        config = json.load(f)
    original_photo_dirs = config['original_photo_dirs']
    dt_keys = config['dt_keys']
    file_types = config['file_types']
    meta_df_path = config['meta_df_path']
    exiftool_executable = config['exiftool_executable']

    # start = datetime.datetime.now()
    # print(f"starting future version: {start}")
    # meta_df = update_meta_df_dask_futures(original_photo_dirs, column_dictionary, file_types, meta_df_path, append=False)
    # end = datetime.datetime.now()
    # print(f"finished future version: {end}")
    # print(f"total time future version: {end-start}")

    print(datetime.datetime.now())
    print("reading")
    meta_df = pd.read_pickle(meta_df_path)
    print(datetime.datetime.now())

    #reduce to dir with tags for testing
    meta_df = meta_df.query("filepath.str.contains('2014-1H')")

    print("splitting")
    meta_df = split_filepath(meta_df)
    print(datetime.datetime.now())
    print("iding")
    meta_id_df = create_id_df(meta_df)
    print(datetime.datetime.now())
    print("merging")
    merged_meta_df = create_merged_meta_df(meta_df)
    meta_df = meta_df.set_index('time_id')
    print(datetime.datetime.now())
    print("tagging")
    filepath_tags = convert_filepath_to_kws(meta_id_df, original_photo_dirs)
    merged_meta_df = add_kws_to_df(filepath_tags, merged_meta_df)

    #print('here')