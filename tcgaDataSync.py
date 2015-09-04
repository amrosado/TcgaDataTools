__author__ = 'arosado'

import pymongo
import gridfs
import re
import json
import requests
import bs4
import tarfile
import datetime
import pdfminer
from PIL import Image

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

    def syncTcgaWithTag(self, tagDict):
        latestArchiveDic = self.getLatestArchive()
        self.handleArchiveContentWithTagDict(latestArchiveDic, tagDict)
        #print statement
        print 'Starting to sync datastore with tag %s' % (str(tagDict))

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

        print('Retrieved latest archive information')
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
        archiveTarExtractFileInfo['tarFilename'] = archiveTarFileInfo['filename']

        return archiveTarExtractFileInfo

    def checkAndUpdateTarExtractFileStatus(self, archiveTarExtractFileInfo, archiveTarExtractFile):
        archiveTarExtractGrid = gridfs.GridFS(self.tcgaGridFsDatabase, 'archiveExtractTarData')
        tcgaArchiveTarExtractFileList = self.tcgaDatabase.get_collection('tcgaTarExtractFileList')

        archiveGridTarExtractFile = None

        archiveTarExtractFileListQuery = tcgaArchiveTarExtractFileList.find(archiveTarExtractFileInfo)

        if archiveTarExtractFileListQuery.count() > 0:
            for matchingTarExtractFileInfo in archiveTarExtractFileListQuery:
                newArchiveGridTarExtractFile = archiveTarExtractGrid.new_file()
                newArchiveGridTarExtractFile.write(archiveTarExtractFile.read())
                newArchiveGridTarExtractFile.filename = archiveTarExtractFileInfo['filename']
                newArchiveGridTarExtractFile.close()

                if 'active' in matchingTarExtractFileInfo:
                    if matchingTarExtractFileInfo['active']:
                        if matchingTarExtractFileInfo['md5'] != newArchiveGridTarExtractFile.md5:
                            matchingTarExtractFileInfo['active'] = False
                            matchingTarExtractFileInfo.close()
                            archiveTarExtractFileInfo['md5'] = newArchiveGridTarExtractFile.md5
                            archiveTarExtractFileInfo['active'] = True
                            archiveTarExtractFileInfo['fileId'] = newArchiveGridTarExtractFile._id
                            tcgaArchiveTarExtractFileList.insert_one(archiveTarExtractFileInfo)
                            archiveGridTarExtractFile = archiveTarExtractGrid.get(newArchiveGridTarExtractFile._id)
                            print 'Updated extracted file %s from tar %s to gridfs' % (archiveTarExtractFileInfo['filename'], archiveTarExtractFileInfo['tarFilename'])
                        else:
                            archiveTarExtractGrid.delete(newArchiveGridTarExtractFile._id)
                            archiveGridTarExtractFile = archiveTarExtractGrid.get(matchingTarExtractFileInfo['fileId'])
                            print 'Retrieved extracted file %s from tar %s to gridfs' % (archiveTarExtractFileInfo['filename'], archiveTarExtractFileInfo['tarFilename'])
        else:
            newArchiveGridTarExtractFile = archiveTarExtractGrid.new_file()
            newArchiveGridTarExtractFile.write(archiveTarExtractFile.read())
            newArchiveGridTarExtractFile.filename = archiveTarExtractFileInfo['filename']
            newArchiveGridTarExtractFile.close()
            archiveTarExtractFileInfo['fileId'] = newArchiveGridTarExtractFile._id
            archiveTarExtractFileInfo['md5'] = newArchiveGridTarExtractFile.md5
            archiveTarExtractFileInfo['active'] = True
            archiveGridTarExtractFile = archiveTarExtractGrid.get(newArchiveGridTarExtractFile._id)
            tcgaArchiveTarExtractFileList.insert_one(archiveTarExtractFileInfo)
            print 'Added extracted file %s from tar %s to gridfs' % (archiveTarExtractFileInfo['filename'], archiveTarExtractFileInfo['tarFilename'])

        return archiveGridTarExtractFile

    def checkAndUpdateTarFileStatus(self, archiveTarFileInfo, archiveTarFileRequest):
        archiveTarGrid = gridfs.GridFS(self.tcgaGridFsDatabase, 'archiveTarData')
        tcgaArchiveTarFileList = self.tcgaDatabase.get_collection('tcgaTarFileList')

        archiveGridTarGz = None

        #Check if file is in database, if different md5 than other file
        #add file and mark old file as inactive
        archiveFileListQuery = tcgaArchiveTarFileList.find(archiveTarFileInfo)
        if archiveFileListQuery.count() > 0:
            for matchingArchiveFileInfo in archiveFileListQuery:
                newArchiveGridTarGz = archiveTarGrid.new_file()
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
                            archiveGridTarGz = archiveTarGrid.get(newArchiveGridTarGz._id)
                            print 'Updated tar file %s from gridfs' % (archiveTarFileInfo['filename'])
                        else:
                            print 'Retrieved tar file %s from gridfs' % (archiveTarFileInfo['filename'])
                            archiveTarGrid.delete(newArchiveGridTarGz._id)
                            archiveGridTarGz = archiveTarGrid.get(matchingArchiveFileInfo['fileId'])
        else:
            newArchiveGridTarGz = archiveTarGrid.new_file()
            newArchiveGridTarGz.write(archiveTarFileRequest.content)
            newArchiveGridTarGz.filename = archiveTarFileInfo['filename']
            newArchiveGridTarGz.close()
            archiveTarFileInfo['fileId'] = newArchiveGridTarGz._id
            archiveTarFileInfo['md5'] = newArchiveGridTarGz.md5
            archiveTarFileInfo['active'] = True
            archiveGridTarGz = archiveTarGrid.get(newArchiveGridTarGz._id)
            tcgaArchiveTarFileList.insert_one(archiveTarFileInfo)
            print('Added tar file %s to gridfs', archiveTarFileInfo['filename'])

        return archiveGridTarGz

    def updateTarFileInfoFromFilename(self, tarFileInfo):
        tarFileNameSplit = tarFileInfo['filename'].split('.')
        if len(tarFileNameSplit) == 9:
            tarFileInfo['level'] = tarFileNameSplit[3].split('_')[1]
            tarFileInfo['version'] = tarFileNameSplit[4] + '.' + tarFileNameSplit[5] + '.' + tarFileNameSplit[6]
        else:
            pass
        return tarFileInfo

    def updateTarExtractFileInfoFromFilename(self, tarExtractFileInfo):
        tarFilenameSplit = tarExtractFileInfo['filename'].split('/')
        tarSecondSplit = tarFilenameSplit[1].split('.')
        if len(tarSecondSplit) == 4:
            tarExtractFileInfo['patientId'] = tarSecondSplit[0]
            tarExtractFileInfo['extractData'] = tarSecondSplit[1]
            tarExtractFileInfo['extractDataType'] = tarSecondSplit[2]
            tarExtractFileInfo['extractDataFileExtension'] = tarSecondSplit[3]
        if len(tarSecondSplit) == 3:
            tarExtractFileInfo['patientId'] = tarSecondSplit[0]
            tarExtractFileInfo['uid'] = tarSecondSplit[1]
            tarExtractFileInfo['extractDataFileExtension'] = tarSecondSplit[2]
        elif len(tarSecondSplit) == 2:
            tarExtractFileInfo['tarInsideInfo'] = tarSecondSplit[0]
            tarExtractFileInfo['tarInsideInfoFileExtension'] = tarSecondSplit[1]
        else:
            pass
        return tarExtractFileInfo

    def handleArchiveTarFile(self, archiveTarFileInfo, archiveTarGridOut):
        #Handle tar file acquired from TCGA archive
        archiveTarFile = tarfile.open(fileobj=archiveTarGridOut, mode='r:gz')
        archiveTarFileMembers = archiveTarFile.getmembers()
        for archiveBzFileMember in archiveTarFileMembers:
            archiveTarMemberInfo = archiveTarFile.getmember(archiveBzFileMember.name)

            newTarFileListInfo = self.generateTarExtractInfoBaseFromTarFileInfo(archiveTarFileInfo)
            newTarFileListInfo['filename'] = archiveTarMemberInfo.name
            newTarFileListInfo = self.updateTarExtractFileInfoFromFilename(newTarFileListInfo)

            tarFileExtractObject = archiveTarFile.extractfile(archiveBzFileMember.name)
            tarFileExtractGridOut = self.checkAndUpdateTarExtractFileStatus(newTarFileListInfo, tarFileExtractObject)

            handleFile = self.handleArchiveFile(newTarFileListInfo, tarFileExtractGridOut)

    def handleArchiveContent(self, archiveDictUsed):

        for archiveContent in archiveDictUsed['data']:
            archiveContentRequest = self.tcgaSession.get(archiveContent[2])
            archiveFileUrl = archiveContent[2]
            archiveTarFileInfo = self.generateTarInformationFromArchiveUrl(archiveFileUrl)
            archiveTarFileInfo = self.updateTarFileInfoFromFilename(archiveTarFileInfo)

            archiveGridTarGz = self.checkAndUpdateTarFileStatus(archiveTarFileInfo, archiveContentRequest)
            archiveTarFileInfo['fileId'] = archiveGridTarGz._id
            handledContent = self.handleArchiveTarFile(archiveTarFileInfo, archiveGridTarGz)

    def handleArchiveContentWithTagDict(self, archiveDictUsed, tagOfInterestDict):

        for archiveContent in archiveDictUsed['data']:
            archiveFileUrl = archiveContent[2]
            archiveTarFileInfo = self.generateTarInformationFromArchiveUrl(archiveFileUrl)
            if archiveTarFileInfo[tagOfInterestDict['type']] == tagOfInterestDict['tag']:
                archiveTarFileInfo = self.updateTarFileInfoFromFilename(archiveTarFileInfo)
                archiveContentRequest = self.tcgaSession.get(archiveContent[2])
                archiveGridTarGz = self.checkAndUpdateTarFileStatus(archiveTarFileInfo, archiveContentRequest)
                archiveTarFileInfo['fileId'] = archiveGridTarGz._id
                handledContent = self.handleArchiveTarFile(archiveTarFileInfo, archiveGridTarGz)

    def handleJustTextInformation(self, tarExtractFileInfo, tarExtractGridOut):
        handledFile = {}

        handledFileText = tarExtractGridOut.read()
        handledFile['text'] = handledFileText
        handledFile['info'] = tarExtractFileInfo
        handledFile['processed'] = False

        return handledFile

    def handleFileWithCsvInformation(self, tarExtractFileInfo, tarExtractGridOut):
        handledFile = {}

        archiveFileText = tarExtractGridOut.read()
        archiveFileCsv = self.processTcgaCsv(archiveFileText)

        handledFile['csv'] = archiveFileCsv
        handledFile['info'] = tarExtractFileInfo
        handledFile['processed'] = True

        return handledFile

    def handleArchiveFile(self, tarExtractFileInfo, tarExtractGridOut):
        handledFile = {}

        if 'tarInsideInfo' in tarExtractFileInfo:
            if tarExtractFileInfo['tarInsideInfo'] == 'CHANGES_DCC':
                handledFile = self.handleJustTextInformation(tarExtractFileInfo, tarExtractGridOut)
            elif tarExtractFileInfo['tarInsideInfo'] == 'DESCRIPTION':
                handledFile = self.handleJustTextInformation(tarExtractFileInfo, tarExtractGridOut)
            elif tarExtractFileInfo['tarInsideInfo'] == 'MANIFEST':
                handledFile = self.handleJustTextInformation(tarExtractFileInfo, tarExtractGridOut)
            elif tarExtractFileInfo['tarInsideInfo'] == 'README_DCC':
                handledFile = self.handleJustTextInformation(tarExtractFileInfo, tarExtractGridOut)
            elif tarExtractFileInfo['tarInsideInfo'] == 'CHANGES':
                handledFile = self.handleJustTextInformation(tarExtractFileInfo, tarExtractGridOut)
            elif tarExtractFileInfo['tarInsideInfo'] == 'CHANGES_DCC':
                handledFile = self.handleJustTextInformation(tarExtractFileInfo, tarExtractGridOut)
            elif tarExtractFileInfo['tarInsideInfo'] == 'DCC_ALTERED_FILES':
                handledFile = self.handleJustTextInformation(tarExtractFileInfo, tarExtractGridOut)
            else:
                handledFile = self.handleJustTextInformation(tarExtractFileInfo, tarExtractGridOut)

            return handledFile

        elif 'patientId' in tarExtractFileInfo:
            if tarExtractFileInfo['extractDataFileExtension'] == 'txt':
                handledFile = self.handleFileWithCsvInformation(tarExtractFileInfo, tarExtractGridOut)
                return handledFile
            if tarExtractFileInfo['extractDataFileExtension'] == 'pdf':
                handledFile['info'] = tarExtractFileInfo
                handledFile['file'] = tarExtractGridOut.read()
                handledFile['processed'] = False
                return handledFile
        else:
            handledFile['info'] = tarExtractFileInfo
            handledFile['file'] = tarExtractGridOut.read()
            handledFile['processed'] = False
            return handledFile

    def __init__(self):
        self.mongoClient = pymongo.MongoClient('localhost', 27017)
        self.tcgaDatabase = self.mongoClient.get_database('TCGA')
        self.tcgaGridFsDatabase = self.mongoClient.get_database('TCGAFiles')
        self.tcgaSession = requests.Session()

tcgaSync = TcgaDataSync()
#tcgaSync.syncTcga()
tagDict = {'type': 'dataInstrument', 'tag': 'pathology_reports'}
tcgaSync.syncTcgaWithTag(tagDict)