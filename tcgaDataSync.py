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

    def generateTarInformationFromArchiveUrl(self, archiveFileUrl):
        #fileInfo
        archiveUrlSplit = archiveFileUrl.split('/')
        archiveFileFtpUploader = archiveUrlSplit[6]
        archiveFileFtpDisease = archiveUrlSplit[7]
        archiveFileFtpDiseaseType = archiveUrlSplit[8]
        archiveFileFtpDiseaseSubType = archiveUrlSplit[9]
        archiveFileFtpUploadingInstitution = archiveUrlSplit[10]
        archiveFileFtpDataInstrument = archiveUrlSplit[11]
        archiveFileFtpDataType = archiveUrlSplit[12]
        archiveFileFtpFilename = archiveUrlSplit[13]
        archiveTarFileInfo = {'uploader': archiveFileFtpUploader, 'disease': archiveFileFtpDisease}
        archiveTarFileInfo['diseaseType'] = archiveFileFtpDiseaseType
        archiveTarFileInfo['diseaseSubType'] = archiveFileFtpDiseaseSubType
        archiveTarFileInfo['uploadingInstitution'] = archiveFileFtpUploadingInstitution
        archiveTarFileInfo['dataInstrument'] = archiveFileFtpDataInstrument
        archiveTarFileInfo['dataType'] = archiveFileFtpDataType
        archiveTarFileInfo['filename'] = archiveFileFtpFilename

        return archiveTarFileInfo

    def generateTarExtractInfoBaseFromTarFileInfo(self, archiveTarFileInfo):
        archiveTarExtractFileInfo = {}
        archiveTarExtractFileInfo['tarFileId'] = archiveTarFileInfo['fileId']
        archiveTarExtractFileInfo['uploader'] = archiveTarFileInfo['uploader']
        archiveTarExtractFileInfo['disease'] = archiveTarFileInfo['disease']
        archiveTarExtractFileInfo['diseaseType'] = archiveTarFileInfo['diseaseType']
        archiveTarExtractFileInfo['diseaseSubType'] = archiveTarFileInfo['diseaseSubType']
        archiveTarExtractFileInfo['uploadingInstitution'] = archiveTarFileInfo['uploadingInstitution']
        archiveTarExtractFileInfo['dataInstrument'] = archiveTarFileInfo['dataInstrument']
        archiveTarExtractFileInfo['dataType'] = archiveTarFileInfo['dataType']
        archiveTarExtractFileInfo['tarFilename'] = archiveTarFileInfo['tarFilename']

        return archiveTarExtractFileInfo

    def checkAndUpdateTarExtractFileStatus(self, archiveTarExtractFileInfo, archiveTarExtractFile):
        archiveTarExtractInfoGrid = gridfs.GridFS(self.tcgaGridFsDatabase, 'archiveExtractTarData')
        tcgaArchiveTarExtractFileList = self.tcgaDatabase.get_collection('tcgaTarExtractFileList')

        archiveGridTarExtractFile = None

        return archiveGridTarExtractFile

    def checkAndUpdateTarFileStatus(self, archiveTarFileInfo, archiveTarFileRequest):
        archiveTarInfoGrid = gridfs.GridFS(self.tcgaGridFsDatabase, 'archiveTarData')
        tcgaArchiveTarFileList = self.tcgaDatabase.get_collection('tcgaTarFileList')

        archiveGridTarGz = None

        #Check if file is in database, if different md5 than other file
        #add file and mark old file as inactive
        archiveFileListQuery = tcgaArchiveTarFileList.find(archiveTarFileInfo)
        if archiveFileListQuery.count() > 0:
            for matchingArchiveFileInfo in archiveFileListQuery:
                newArchiveGridTarGz = archiveTarInfoGrid.new_file()
                newArchiveGridTarGz.write(archiveTarFileRequest.content)
                newArchiveGridTarGz.filename = archiveTarFileInfo['filename']
                newArchiveGridTarGz.close()
                if 'active' in matchingArchiveFileInfo:
                    if matchingArchiveFileInfo['active']:
                        if matchingArchiveFileInfo['md5'] != newArchiveGridTarGz.md5:
                            matchingArchiveFileInfo['active'] = False
                            matchingArchiveFileInfo.close()
                            archiveTarFileInfo['md5'] = newArchiveGridTarGz.md5
                            archiveTarFileInfo['active'] = True
                            archiveTarFileInfo['fileId'] = newArchiveGridTarGz._id
                            tcgaArchiveTarFileList.insert_one(archiveTarFileInfo)
                            archiveGridTarGz = archiveTarInfoGrid.get(newArchiveGridTarGz._id)
                        else:
                            archiveTarInfoGrid.delete(newArchiveGridTarGz._id)
                            archiveGridTarGz = archiveTarInfoGrid.get(matchingArchiveFileInfo['fileId'])
        else:
            newArchiveGridTarGz = archiveTarInfoGrid.new_file()
            newArchiveGridTarGz.write(archiveTarFileRequest.content)
            newArchiveGridTarGz.filename = archiveTarFileInfo['filename']
            newArchiveGridTarGz.close()
            archiveTarFileInfo['fileId'] = newArchiveGridTarGz._id
            archiveTarFileInfo['md5'] = newArchiveGridTarGz.md5
            archiveTarFileInfo['active'] = True
            archiveGridTarGz = archiveTarInfoGrid.get(newArchiveGridTarGz._id)
            tcgaArchiveTarFileList.insert_one(archiveTarFileInfo)

        return archiveGridTarGz

    def handleArchiveTarFile(self, archiveTarFileInfo, archiveTarGridOut):
        #Handle tar file acquired from TCGA archive
        archiveTarFile = tarfile.open(fileobj=archiveTarGridOut, mode='r:gz')
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

    def handleArchiveContent(self, archiveDictUsed):

        for archiveContent in archiveDictUsed['data']:
            archiveContentRequest = self.tcgaSession.get(archiveContent[2])
            archiveFileUrl = archiveContent[2]
            archiveTarFileInfo = self.generateInformationFromArchiveUrl(archiveFileUrl)

            archiveGridTarGz = self.checkAndUpdateTarFileStatus(archiveTarFileInfo, archiveContentRequest)

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