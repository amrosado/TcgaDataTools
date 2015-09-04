__author__ = 'arosado'

import pymongo
import gridfs
import re
import json
import requests
import bs4
import tarfile
import datetime

class TcgaDataSync:
    tcgaDatabase = None
    tcgaGridFsDatabase = None
    mongoClient = None
    tcgaSession = None

    tcgaLatestArchiveUrl = 'http://tcga-data.nci.nih.gov/datareports/resources/latestarchive'

    def processTcgaCsv(self, csv):
        firstLine = True
        infoLine = False
        fieldNames = []
        data = []
        csvDict = {}
        csvLines = csv.split('\n')

        for line in csvLines:
            lineBreakdown = line.split('\t')
            if line == '':
                pass
            elif len(lineBreakdown) > 1:
                if firstLine:
                    for fieldName in lineBreakdown:
                        fieldNames.append(fieldName)
                    firstLine = False
                else:
                    tempDataHolder = []
                    for fieldData in lineBreakdown:
                        tempDataHolder.append(fieldData)
                    data.append(tempDataHolder)

        csvDict['data'] = data
        csvDict['fieldNames'] = fieldNames

        return csvDict

    def syncTcga(self):
        latestArchiveDic = self.getLatestArchive()
        self.handleArchiveContent(latestArchiveDic)

    def getLatestArchive(self):
        currentDateTime = datetime.datetime.now()
        #self.tcgaDatabase.drop_collection('tcgaArchivesInfo')
        tcgaArchiveInfo = self.tcgaDatabase.get_collection('tcgaArchivesInfo')
        pastArchiveInfoGrid = gridfs.GridFS(self.tcgaGridFsDatabase, 'pastArchiveData')
        latestArchiveRequest = self.tcgaSession.get(self.tcgaLatestArchiveUrl)

        latestArchiveCsvDict = self.processTcgaCsv(latestArchiveRequest.content)

        allArchiveInfo = tcgaArchiveInfo.find()

        if allArchiveInfo != None:
            for archiveInfo in allArchiveInfo:
                archiveInfoDateTime = archiveInfo['date']
                timeDifference = currentDateTime - archiveInfoDateTime
                if timeDifference.days > 7:
                    tcgaArchiveInfo.insert_one({'fieldNames': latestArchiveCsvDict['fieldNames'], 'data': latestArchiveCsvDict['data'], 'date': currentDateTime})
                else:
                    archiveDictUsed = archiveInfo
        if allArchiveInfo.retrieved == 0:
            tcgaArchiveInfo.insert_one({'fieldNames': latestArchiveCsvDict['fieldNames'], 'data': latestArchiveCsvDict['data'], 'date': currentDateTime})
            archiveDictUsed = latestArchiveCsvDict

        return archiveDictUsed

    def handleArchiveContent(self, archiveDictUsed):
        archiveInfoGrid = gridfs.GridFS(self.tcgaGridFsDatabase, 'archiveData')
        tcgaArchiveFileList = self.tcgaDatabase.get_collection('tcgaFileList')

        for archiveContent in archiveDictUsed['data']:
            archiveContentRequest = self.tcgaSession.get(archiveContent[2])
            archiveUrlSplit = archiveContent[2].split('/')

            #fileInfo
            archiveFileFtpUploader = archiveUrlSplit[6]
            archiveFileFtpDisease = archiveUrlSplit[7]
            archiveFileFtpDiseaseType = archiveUrlSplit[8]
            archiveFileFtpDiseaseSubType = archiveUrlSplit[9]
            archiveFileFtpUploadingInstitution = archiveUrlSplit[10]
            archiveFileFtpDataInstrument = archiveUrlSplit[11]
            archiveFileFtpDataType = archiveUrlSplit[12]
            archiveFileFtpFilename = archiveUrlSplit[13]
            archiveFileInfo = {'uploader': archiveFileFtpUploader, 'disease': archiveFileFtpDisease}
            archiveFileInfo['diseaseType'] = archiveFileFtpDiseaseType
            archiveFileInfo['diseaseSubType'] = archiveFileFtpDiseaseSubType
            archiveFileInfo['uploadingInstitution'] = archiveFileFtpUploadingInstitution
            archiveFileInfo['dataInstrument'] = archiveFileFtpDataInstrument
            archiveFileInfo['dataType'] = archiveFileFtpDataType
            archiveFileInfo['filename'] = archiveFileFtpFilename

            #Grid fs for archive file
            newArchiveGridTarGz = archiveInfoGrid.new_file()
            newArchiveGridTarGz.write(archiveContentRequest.content)
            newArchiveGridTarGz.filename = archiveFileFtpFilename
            newArchiveGridTarGz.close()
            archiveGridTarGz = archiveInfoGrid.get(newArchiveGridTarGz._id)
            if archiveInfoGrid.exists({'filename': archiveFileFtpFilename}):
                archiveGridTarGzQuery = archiveInfoGrid.find({'filename': archiveFileFtpFilename})
                for matchingArchiveFile in archiveGridTarGzQuery:
                    if (newArchiveGridTarGz.md5 != archiveGridTarGz.md5):
                        archiveInfoGrid.delete(archiveGridTarGz._id)
                    else:
                        archiveGridTarGz = archiveInfoGrid.get(newArchiveGridTarGz._id)
            else:
                tcgaArchiveFileList.insert_one({'filename': archiveFileName, 'active': True, 'fileId':archiveGridTarGz._id})
            archiveTarFile = tarfile.open(fileobj=archiveGridTarGz, mode='r:gz')
            archiveTarFileMembers = archiveTarFile.getmembers()
            for archiveBzFileMember in archiveTarFileMembers:
                archiveTarFileInfo = archiveTarFile.getmember(archiveBzFileMember.name)
                newArchiveGridFile = archiveInfoGrid.new_file()
                newArchiveGridFile.filename = archiveTarFileInfo.name
                archiveFile = archiveTarFile.extractfile(archiveTarFileInfo)
                newArchiveGridFile.write(archiveFile.read())
                newArchiveGridFile.close()
                newArchiveGridOut = archiveInfoGrid.get(newArchiveGridFile._id)
                if archiveInfoGrid.exists({'filename': archiveTarFileInfo.name}):
                    archiveInfoGridFind = archiveInfoGrid.find_one({'filename': archiveTarFileInfo.name})
                    if (archiveInfoGridFind.md5 != newArchiveGridOut.md5):
                        archiveInfoGrid.delete(archiveInfoGridFind._id)
                        newArchiveGridFile.close()
                        archiveFileGridOutReturn = newArchiveGridOut
                    if (archiveInfoGridFind.md5 == newArchiveGridOut.md5):
                        archiveInfoGrid.delete(newArchiveGridOut._id)
                        archiveFileGridOutReturn = archiveInfoGridFind
                else:
                    archiveFileGridOutReturn = newArchiveGridOut
                self.handleArchiveFile(archiveFileGridOutReturn)

    def handleArchiveFile(self, archiveBzFile):
        archiveName = archiveBzFile.name
        archiveNameSplit = archiveName.split('/')
        archiveFileName = archiveNameSplit[1]
        archiveDirName = archiveNameSplit[0]
        archiveInfoFileNameSplit = archiveFileName.split('.')
        archiveDirInfoSplit = archiveDirName.split('.')

        if len(archiveDirInfoSplit) > 0:
            pass
        if archiveFileName == 'CHANGES_DCC.txt':
            pass
        elif archiveFileName == 'DESCRIPTION.txt':
            pass
        elif archiveFileName == 'MANIFEST.txt':
            pass
        elif archiveFileName == 'README_DCC.txt':
            pass
        elif archiveFileName == 'CHANGES.txt':
            pass
        elif archiveFileName == 'CHANGES_DCC.txt':
            pass
        elif archiveFileName == 'DCC_ALTERED_FILES.txt':
            pass
        elif len(archiveInfoFileNameSplit) > 2:
            archiveFileText = archiveBzFile.read()
            archiveFileCsv = self.processTcgaCsv(archiveFileText)
            pass
        pass

    def __init__(self):
        self.mongoClient = pymongo.MongoClient('localhost', 27017)
        self.tcgaDatabase = self.mongoClient.get_database('TCGA')
        self.tcgaGridFsDatabase = self.mongoClient.get_database('TCGAFiles')
        self.tcgaSession = requests.Session()

tcgaSync = TcgaDataSync()
tcgaSync.syncTcga()