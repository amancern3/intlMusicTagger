import json
import os
import math
import pydub
from pydub import AudioSegment
from compmusic import dunya
from datetime import datetime

class OSFunctionality:
    '''
    Child class to hold any common functions used for both Downloader and Splitter classes.

    - dunya_config (str): path to dunya config to authenticate.
    '''
    def __init__(self, dunya_config:str):
        self.dunya_config = dunya_config
    
    def _authenticate(self):
        '''
        Authenticate user for access to Dunya API. API key must be in "key".
        '''
        with open(self.dunya_config, 'r') as j:
            j = json.load(j)
        dunya.set_token(j['key'])
        print('Authenticated')

    def _check_make_dir(self, path):
        '''Check if save directory exists. If it doesn't, make it.'''
        if not os.path.isdir(path):
            os.mkdir(path)

class Downloader(OSFunctionality):
    '''
    Downloader class to download first N files from both Andalusian and Hindustani
    datsets.

    params:
        - N (int): number of files to download from each set.
        - dunya_config (str): path to dunya config for authentication.
    '''
    def __init__(
        self, 
        N:int, 
        dunya_config:str,
        dataset:str,
        path_to_andalusian:str = os.path.join('data', 'andalusian'), 
        path_to_hindustani:str = os.path.join('data', 'hindustani'),
        configs_save_path:str = os.path.join('configs'),
        start_from:int = 0
    ):
        super().__init__(dunya_config)
        # Instantiate params:
        self.N = N
        self.start_from = start_from
        self.dataset = dataset
        
        # Create paths if they don't exit:
        self.path_to_andalusian = path_to_andalusian
        self._check_make_dir(self.path_to_andalusian)
        self.path_to_hindustani = path_to_hindustani
        self._check_make_dir(self.path_to_hindustani)
        self.configs_save_path = configs_save_path
        self._check_make_dir(self.configs_save_path)
        
        # Instantiate Andalusian and Hindustani modules for later on
        self.andalusian = dunya.andalusian
        self.hindustani = dunya.hindustani

        # Authenticate with config json:
        self._authenticate()
    
    def _save_recordings(self, recordings_list:list, config_name:str):
        '''
        Quick method to save list of jsons to specified path.

        params:
            - recordings_list (list): list of recordings for all N segments
            - config_name (str): name of json file to save all information
        '''
        with open(config_name, 'w') as j:
            json.dump(recordings_list, j)
    
    def _convert_name(self, path_to_folder:str, song_title:str, mbid:str):
        '''
        Convert name of specified mp3 file.

        params:
            - path_to_folder (str): folder name. will be either pointing to andalusian or hindustani directories.
            - song_title (str): song title of song to be converted. dunya api will save the mp3 as song_title.
            - mbid (str): unique mbid of song. will be used for new save name as it will be more consistent.
        '''
        # Create paths:
        mp3_path = os.path.join(path_to_folder, song_title)
        wav_path = os.path.join(path_to_folder, f'{mbid}.mp3')
        # Convert audio file:
        AudioSegment.from_file(mp3_path, format='mp3').export(wav_path, format='mp3')
        # Remove mp3 file:
        os.remove(mp3_path)

    def _download(self, recordings):
        '''
        Actually download in the loop. This helps with redundant code.
        '''
        for i in range(self.start_from, (self.N + self.start_from)):
            # Get current entry:
            entry = recordings[i]
            
            # Download file
            print(f'Downloading {entry["title"]} from {self.dataset.capitalize()}.')
            if self.dataset == 'hindustani':
                name = self.hindustani.download_mp3(entry['mbid'], self.path_to_hindustani)
            elif self.dataset == 'andalusian':
                name = self.andalusian.download_mp3(entry['mbid'], self.path_to_andalusian)
            print('Download complete.')

            # Convert name
            if self.dataset == 'andalusian':
                self._convert_name(self.path_to_andalusian, name, entry['mbid'])
            elif self.dataset == 'hindustani':
                self._convert_name(self.path_to_hindustani, name, entry['mbid'])
            

    
    def download(self):
        '''
        Core method for downloading the first N files from both Andalusian and Hindustani corpora.
        '''
        # Get all recordings. Each entry of the list will be a JSON with a recording ID
        if self.dataset == 'andalusian':
            self._download(self.andalusian.get_recordings())
        elif self.dataset == 'hindustani':
            self._download(self.hindustani.get_recordings())
        

class Splitter(OSFunctionality):
    '''
    Splitter class to iterate through all of the audio files within a directory and splits into specified duration.

    params:
        - dir_path (str): path of directory with full audio files
        - save_path (str): path of directory to save
        of len_minutes_crop. can be either int or float.
        - len_minutes_crop (float): number of minutes of each segment to crop to if recording segment is too long. can be either int or float.
        - len_large_segment (float): length in minutes of a large segement to be cropped down further for andalusian data.
        - dunya_config (str): path to dunya config to authenticate.
        - sr (float): sample rate of audio. default set to 16000
    '''
    def __init__(self, dir_path:str, save_path:str, len_minutes_crop:float, len_large_segment:float, dunya_config:str):
        super().__init__(dunya_config)
        # Instantiate params
        self.dir_path = dir_path
        self.save_path = save_path
        self.len_minutes_crop = len_minutes_crop
        self.len_large_segment = len_large_segment
        # Convert minutes to num samples
        self.num_samples = self.len_minutes_crop * 60 * 1000
        self.len_large_segment *= 60 * 1000

        # Sanity checks:

        # Assert directory exists
        assert os.path.isdir(self.dir_path), f"Specified directory {self.dir_path} does not exist!"
        # Ensure the save directory exists:
        self._check_make_dir(self.save_path)
    
    def _get_common_names_processed(self, recordings_info:list):
        common_names = [recordings_info[i]['common_name'] for i in range(len(recordings_info))]
        processed_str = '/-/-'.join(common_names)
        return processed_str

    def _split_hindustani(self, audio, full_file_name):
        '''Split on Hindustani dataset.'''
        # Get metadata:
        print('Getting recordings')
        recordings = dunya.hindustani.get_recording(full_file_name)
        print('Completed. Now splitting data')
        layas = self._get_common_names_processed(recordings['layas'])
        taals = self._get_common_names_processed(recordings['taals'])
        forms = self._get_common_names_processed(recordings['forms'])
        tags_ = {
            'genre': 'hindustani',
            'layas': [layas],
            'taals': [taals],
            'forms': [forms]
        }
        # Calculate number of (len_minutes) segments in the audio file
        num_segments = math.floor(len(audio) / self.num_samples)
        # If we can't parse 2 or more segments, no need to split:
        if num_segments <= 1:
            seg_i = 0
            file_name = f'{full_file_name}_{seg_i}.mp3'
            segment_path = os.path.join(self.save_path, file_name)
            audio.export(segment_path, format='mp3', tags=tags_)
        else:
            # Loop through segment index and save
            for seg_i in range(num_segments):
                # Get start and end
                start = seg_i*self.num_samples
                end = ((seg_i + 1) * self.num_samples) - 1
                # Segment array
                audio_segment = audio[start:end]
                # Write array:
                file_name = f'{full_file_name}_{seg_i}.mp3'
                segment_path = os.path.join(self.save_path, file_name)
                audio_segment.export(segment_path, format='mp3', tags=tags_)
        print('Splitting completed.')
    
    def _split_andalusian(self, audio, full_file_name):
        '''Split on Andalusian dataset.'''
        # Get recording info and the sections:
        print('Getting recordings')
        recording_info = dunya.andalusian.get_recording(full_file_name)
        sections = recording_info['sections']
        print('Completed. Now splitting data')
        # Loop through sections
        seg_i = 0
        for section in sections:
            
            # Get start and end time
            st = datetime.strptime(section['start_time'], '%H:%M:%S')
            et = datetime.strptime(section['end_time'], '%H:%M:%S')
            
            # Now convert to array index:
            st, et = self._datetime_to_index(st), self._datetime_to_index(et)
            audio_segment = audio[st:et]
            
            # Get mizan, nawba, and form:
            mizan = section['mizan']['display_order']
            nawba = section['nawba']['display_order']
            form = section['form']['display_order']
            
            # Now see if the segment is too big:
            if len(audio_segment) >= self.len_large_segment:
                # Update seg_i for indices of segment:
                    seg_i = self._segment_split(
                    st, et, full_file_name, audio_segment, mizan, nawba, form, seg_i
                    ) # This function will save all of the data as well.
            else:
                # Construct segment
                seg_i += 1
                file_name = f'{full_file_name}_{seg_i}.mp3'
                segment_path = os.path.join(self.save_path, file_name)
                tags_ = {
                    'genre': 'andalusian',
                    'mizan': mizan,
                    'nawba': nawba,
                    'form': form
                }
                audio_segment.export(segment_path, format='mp3', tags=tags_)
    
    def _segment_split(self, st, et, full_file_name, audio_segment, mizan, nawba, form, seg_i):
        '''
        Split segment further.

        params:
            - st (float): start time of array (index)
            - et (float): end time of array (index)
            - audio_segment (np.ndarray): numpy array of audio segment
            - mizan (dict): dict containing mizan
            - nawba (dict): dict containing nawba
            - form (dict): dict containing form
        '''
        num_segments = math.floor(len(audio_segment) / self.num_samples)
        for seg in range(num_segments):
            # Get start index:
            start_ = st + (seg*self.num_samples)
            # If its the last segment, use et as final index
            if seg == num_segments - 1:
                end_ = et
            # Otherwise, calculate the end index of the segment
            else:
                end_ = st + ((seg+1)*self.num_samples) - 1
            
            # Update segment index and file name and update file names list:
            seg_i += 1
            file_name = f'{full_file_name}_{seg_i}.mp3'
            segment_path = os.path.join(self.save_path, file_name)

            # Construct tags:
            tags_ = {
                'genre': 'andalusian',
                'mizan': mizan,
                'nawba': nawba,
                'form': form
            }
            
            # Now do the splitting:
            audio_sub_segment = audio_segment[start_:end_]
            audio_sub_segment.export(segment_path, format='mp3', tags=tags_)
        # Return all updated information when complete:
        return seg_i

    def _datetime_to_index(self, dt:datetime) -> float:
        '''Convert dt to seconds, add them up, and multiply by 1000'''
        hour_ = dt.hour * 3600
        minute_ = dt.minute * 60
        total = hour_ + minute_ + dt.second
        return total * 1000

    def split(self):
        '''Main split function:'''
        # Authenticate
        self._authenticate()
        data_folder = self.dir_path.split('/')[-1]
        for file in os.listdir(self.dir_path):
            file_name = file.split('.')[0] # Will be mbid
            if file_name == '':
                continue
            # Load audio as array
            print(f'Loading {file_name}')
            audio = pydub.AudioSegment.from_mp3(os.path.join(self.dir_path, file))
            print(f'Loaded. \n Processing {file_name}')
            if data_folder == 'andalusian':
                self._split_andalusian(audio, file_name)
            elif data_folder == 'hindustani':
                self._split_hindustani(audio, file_name)

# if __name__ == '__main__':
#     dunya_config = 'configs/dunya_config.json'
    
#     start_from = 110
#     N = 10
#     downloader = Downloader(N, dunya_config, dataset='andalusian', start_from=start_from)
#     downloader.download()

#     dir_path = 'data/andalusian'
#     save_path = 'data/andalusian_crop'
#     splitter = Splitter(dir_path, save_path, 3, 6, dunya_config)
#     splitter.split()