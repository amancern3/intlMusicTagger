from datetime import datetime
from time import sleep
from tokenize import String
from venv import create

import hashlib
import pandas as pd
import requests
import os
import argparse
import json

from dotenv import load_dotenv
from flask import Flask, request, json, redirect, url_for
from hashlib import sha512
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

#env vars
load_dotenv()
access_token = os.getenv("access_token")
secret = os.getenv("secret")    
request_url = "https://api.cyanite.ai/graphql"

#def/dummy functions
def startProcessProxy(dirName, path_to_csv):
    startProcess(dirName, path_to_csv)

def startProcess(dirName, path_to_csv):
 
    #init client 
    transport = AIOHTTPTransport(url=request_url, headers = { "Authorization": "Bearer {}".format(access_token)})
    client = Client(transport=transport, fetch_schema_from_transport=True)

    
    #get data
    files = file_from_csv(path_to_csv)
    hashedFiles = hashFiles(files)

    #loop for file
    for file in files:
      fullFile = os.path.join(dirName, file)
      fileName = file.split(".")[0]
      _id, uploadUrl = uploadRequest(client)
      uploadFiles(fullFile, _id, uploadUrl)
      trackID = createTrack(client, _id, fileName)
      
      #retriveIDs(hashedFiles, client)
      getFeatures(client, trackID, fileName)
      #redirect(url_for('uploadFiles'))
      
      

#def/dummy functions
def run():
    print("here")
    return 200

#params : None
#fn: sends upload request to API endpoint
#return: 'end' [str] -> denotes end of entire process.
def uploadRequest(client):
    
    print("Sending Upload Request........")
    query = gql(
        """
        mutation FileUploadRequestMutation {
            fileUploadRequest {
                id
                uploadUrl
            }
        }
        """
    )
    
    result = client.execute(query)
    
    _id = result['fileUploadRequest']['id']
    uploadUrl = result['fileUploadRequest']['uploadUrl']    
    
    timestamp = str(datetime.now())
    result = {timestamp: result}
    
    print("Getting Upload Creds......")
    a_dict = {timestamp: result}
    
    with open('uploadRequest.json') as f:
        data = json.load(f)
    data.update(a_dict)

    with open('uploadRequest.json', 'w') as f:
        json.dump(data, f)
    
    

    return _id, uploadUrl

#params : None
#fn: converts dir to list
#return: list
def file_from_csv(path_to_csv):
    df = pd.read_csv(path_to_csv)
    return list(df['filename'])

def file2List(dir_path):
    files = os.listdir(dir_path) 
    return files 


#params : MP3 DIR
#fn: converts dir to hashedlist
#return: list
def hashFiles(files):
    hashedList=[hashlib.sha256(i.encode()).hexdigest().upper() for i in files]
    return hashedList


#params : MP3 DIR
#fn: uploads file to API endpoint
#return: None
def uploadFiles(file, _id, uploadUrl):
    
    print("Uploading files......")

  
    params = {
        'Content-Type': 'audio/mpeg',
        'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
        'X-Amz-Credential': 'AKIAJEAGRZG3TDV5AMHQ/20210204/eu-central-1/s3/aws4_request',
        'X-Amz-Date': '20210204T132927Z',
        'X-Amz-Expires': '900',
        'X-Amz-Signature': '8d8e8b8029dc2710e1ca27be1ef9094801e4e8bd7c0c313ac40da30a56330558',
        'X-Amz-SignedHeaders': 'host',
    }

    #add sha256 check -> upload only nonexistant
    with open(file, 'rb') as f:
        data = f.read()

    response = requests.put(uploadUrl, params=params, data=data)
 
    print(response.json)
    print(response.status_code)
    return 


#params : Client[gql]
#fn: creates track of uploadedFiles
#return: None
def createTrack(client, _id, fileName):
    print("Creating Track......")

    query = gql(
    """
       mutation LibraryTrackCreateMutation($input: LibraryTrackCreateInput! ) {
  libraryTrackCreate(input: $input) {
    __typename
    ... on LibraryTrackCreateSuccess {
      createdLibraryTrack {
        id
      }
    }
    ... on LibraryTrackCreateError {
      code
      message
    }
  }
}

    
    """
    )
    
    #change hardcoded vars for params
    
    params = { "input": { "uploadId": _id, "title": fileName } }
    result = client.execute(query, variable_values = params)
    trackID = result["libraryTrackCreate"]["createdLibraryTrack"]["id"]
    
    #per file json dump 
    timestamp = str(datetime.now())
    result = {timestamp: result}
    
    a_dict = {timestamp: result}
    
    with open('createTrack.json') as f:
        data = json.load(f)
    data.update(a_dict)

    with open('createTrack.json', 'w') as f:
        json.dump(data, f)
    
    print(result)
    
    return trackID
    

#params : Clinet[gql], hashedFiles
#fn: retrives ID to check track existance
#return: None  [shld return something]
def retriveIDs(hashedFiles, client):

    print("Retriving IDs............")
    query = gql(
        """
        query LibraryTracksFilteredBySHA256Query($sha256: String!) {
    libraryTracks(filter: { sha256: $sha256 }) {
        pageInfo {
        hasNextPage
        }
        edges {
        cursor
        node {
            id
            title
        }
        }
    }
    }
    """   
    )
    
    #change hardcoded vars
    params = {"sha256": "e5118512f23f922c54ef92d995f8fe7da7046819390f0d02ba596fd0a3db67e2"}
    result = client.execute(query, variable_values = params)
    timestamp = str(datetime.now())

    a_dict = {timestamp: result}
    
    with open('retriveIDs.json') as f:
        data = json.load(f)
    data.update(a_dict)

    with open('retriveIDs.json', 'w') as f:
        json.dump(data, f)
    
    #result = {timestamp: result}
    
    
    #with open("retriveIDs.json", "w") as write_file:
    #    result = json.update(result, write_file)
    #print(result )
    
    #add return for check 

#PARAMS: [IMPLEMENT] ID(s), Client(gql)
#FN : FN TO QUERY FOR CREATED TRACK FEATURES
#RETURN: JSON PAYLOAD
def getFeatures(client, trackID, fileName):
    
    sleep(45)
    
    query = gql(
    """
    query LibraryTrackQuery($libraryTrackId: ID!) {
  libraryTrack(id: $libraryTrackId) {
    __typename
    ... on LibraryTrackNotFoundError {
      message
    }
    ... on LibraryTrack {
      id
      title
      audioAnalysisV6 {
        __typename
        ... on AudioAnalysisV6Finished {
          result {
            valence
            arousal
            energyLevel
            energyDynamics
            emotionalProfile
            emotionalDynamics
            mood {
              aggressive
              calm
              chilled
              dark
              energetic
              epic
              happy
              romantic
              sad
              scary
              sexy
              ethereal
              uplifting
            }
            moodTags
            moodMaxTimes {
              mood
              start
              end
            }
            moodAdvanced {
              anxious
              barren
              cold
              creepy
              dark
              disturbing
              eerie
              evil
              fearful
              mysterious
              nervous
              restless
              spooky
              strange
              supernatural
              suspenseful
              tense
              weird
              aggressive
              agitated
              angry
              dangerous
              fiery
              intense
              passionate
              ponderous
              violent
              comedic
              eccentric
              funny
              mischievous
              quirky
              whimsical
              boisterous
              boingy
              bright
              celebratory
              cheerful
              excited
              feelGood
              fun
              happy
              joyous
              lighthearted
              perky
              playful
              rollicking
              upbeat
              calm
              contented
              dreamy
              introspective
              laidBack
              leisurely
              lyrical
              peaceful
              quiet
              relaxed
              serene
              soothing
              spiritual
              tranquil
              bittersweet
              blue
              depressing
              gloomy
              heavy
              lonely
              melancholic
              mournful
              poignant
              sad
              frightening
              horror
              menacing
              nightmarish
              ominous
              panicStricken
              scary
              concerned
              determined
              dignified
              emotional
              noble
              serious
              solemn
              thoughtful
              cool
              seductive
              sexy
              adventurous
              confident
              courageous
              resolute
              energetic
              epic
              exciting
              exhilarating
              heroic
              majestic
              powerful
              prestigious
              relentless
              strong
              triumphant
              victorious
              delicate
              graceful
              hopeful
              innocent
              intimate
              kind
              light
              loving
              nostalgic
              reflective
              romantic
              sentimental
              soft
              sweet
              tender
              warm
              anthemic
              aweInspiring
              euphoric
              inspirational
              motivational
              optimistic
              positive
              proud
              soaring
              uplifting
            }
            moodAdvancedTags
            movement {
              bouncy
              driving
              flowing
              groovy
              nonrhythmic
              pulsing
              robotic
              running
              steady
              stomping
            }
            movementTags
            bpmPrediction {
              value
              confidence
            }
            bpmRangeAdjusted
          }
        }
      }
    }
  }
}
    """
        
    )
    params = {"libraryTrackId": trackID}
    result = client.execute(query, variable_values = params)
    
    # Check if directory for jsons does not exist. If it doesn't create it:
    if not os.path.isdir('classifierResults'):
      os.mkdir('classifierResults')
    
    
    
    # Save json as fileName.json
    with open(os.path.join('classifierResults', f'{fileName}.json'), 'w') as j:
      json.dump(result, j)
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Pass in directory name.')

    parser.add_argument('--dir_name', help='Name of directory with mp3s')
    parser.add_argument('--csv_name', help='Path to csv')

    args = parser.parse_args()

    startProcessProxy(args.dir_name, args.csv_name)
    