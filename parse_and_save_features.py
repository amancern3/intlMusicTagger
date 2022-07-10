import json
import pandas as pd
import os

def load_json(json_path):
    '''Load json and return json object.'''
    with open(json_path, 'r') as j:
        j = json.load(j)
    return j


def process_features(csv_path, jsons_dir, csv_save_path):
    '''Process features given the filename.'''
    df = pd.read_csv(csv_path)
    jsons_ = []
    drop = []
    for i in range(len(df)):
        # Get file, filename and load json
        file_ = df['filename'][i]
        filename = file_.split('.')[0]
        json_path = filename + '.json'
        feature_json = load_json(os.path.join(jsons_dir, json_path))
        # Parse out results from feature json. If no result, it did not process in time.
        try:
            result = feature_json['libraryTrack']['audioAnalysisV6']['result']
            jsons_.append(result)
        except:
            # Drop index in pain dataframe
            drop.append(i)
        # Convert jsons to dataframe, and save
    feature_df = pd.json_normalize(jsons_)
    feature_df = feature_df.drop(['moodAdvancedTags', 'moodMaxTimes', 'moodTags', 'movementTags'], axis=1)
    df = df.drop(drop, axis=0).reset_index()
    df = df.join(feature_df)
    df = df.drop('index', axis=1)
    df.to_csv(csv_save_path, index=None)


if __name__ == '__main__':
    CSV_PATH = 'csvs/hindustani_crop.csv'
    JSONS_DIR = 'classifierResults'
    SAVE_PATH = 'csvs/hindustani_features.csv'
    process_features(CSV_PATH, JSONS_DIR, SAVE_PATH)
