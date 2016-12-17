"""
This code is used to convert HDF5 file to CSV, written by Kumari Shalini
"""

import sys
import os
import glob
import hdf5_getters
import re

def main():
    basedir = "./../songMetaInfo.txt"

    ext = ".h5"

    if len(sys.argv) > 1:
        basedir = sys.argv[1]

    outputfile = 'SongFileMetaData.csv'

    if len(sys.argv) > 2 :
        outputfile = sys.argv[2]


    csvWriter = open(outputfile, 'w')

    csvWriter.write("title,songId,artistId,artistfamilarity,artistHotness,songHotness,"+
                      "songEnfOfFadeIn,startFadeout,energy,loudness,albumID,albumName,artistName,danceability,duration,keySignatureConfidence,tempo,timeSignature,timeSignatureConfidence,year\n")

    with open(basedir) as file:
        for line in file.readlines():
            f = line.strip()
            #newf = f + "text"
            print f
            #print f
            try:
                songH5File = hdf5_getters.open_h5_file_read(f)
                csvStr = ""
                #0
                title= str(hdf5_getters.get_title(songH5File))
                csvStr += title + ","
                #1
                songId = str(hdf5_getters.get_song_id(songH5File))
                csvStr += songId + ","
                #2
                artistId = str(hdf5_getters.get_artist_id(songH5File))
                csvStr += artistId + ","
                #3
                artistfamilarity = str(hdf5_getters.get_artist_familiarity(songH5File))
                csvStr += artistfamilarity + ","
                #4
                artistHotness = str(hdf5_getters.get_artist_hotttnesss(songH5File))
                csvStr += artistHotness + ","
                #5
                songHotness = str(hdf5_getters.get_song_hotttnesss(songH5File) )
                csvStr += songHotness + ","
                #6
                songEnfOfFadeIn = str(hdf5_getters.get_end_of_fade_in(songH5File))
                csvStr += songEnfOfFadeIn + ","
                #7
                startFadeOut = str(hdf5_getters.get_start_of_fade_out(songH5File))
                csvStr += startFadeOut + ","
                #8
                energy = str(hdf5_getters.get_energy(songH5File))
                csvStr += energy + ","
                #9
                loudness = str(hdf5_getters.get_loudness(songH5File))
                csvStr += loudness + ","
                #10
                albumID = str(hdf5_getters.get_release_7digitalid(songH5File))
                csvStr += albumID + ","
                #11
                albumName = str(hdf5_getters.get_release(songH5File))
                csvStr += albumName + ","
                #12
                artistName = str(hdf5_getters.get_artist_name(songH5File))
                csvStr += artistName + ","
                #13
                danceability = str(hdf5_getters.get_danceability(songH5File))
                csvStr += danceability + ","
                #14
                duration = str(hdf5_getters.get_duration(songH5File))
                csvStr += duration + ","
                #15
                keySignatureConfidence = str(hdf5_getters.get_key_confidence(songH5File))
                csvStr += keySignatureConfidence + ","
                #16
                tempo = str(hdf5_getters.get_tempo(songH5File))
                csvStr += tempo + ","
                ## 17
                timeSignature = str(hdf5_getters.get_time_signature(songH5File))
                csvStr += timeSignature + ","
                #18
                timeSignatureConfidence = str(hdf5_getters.get_time_signature_confidence(songH5File))
                csvStr += timeSignatureConfidence + ","
                #19
                year = str(hdf5_getters.get_year(songH5File))
                csvStr += year + ","
                #print song count
                csvStr += "\n"
                csvWriter.write(csvStr)
                #print csvStr

                songH5File.close()
            except:
                print "Error in processing file"

        csvWriter.close()
	
main()
