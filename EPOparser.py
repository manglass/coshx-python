from lxml import etree

import os

import zipfile

import datetime

# os.getcwd()
# 

# dir(etree)

# list( map(lambda x: x.attrib, tree.xpath('//abstract')) )[0]
# list( map(lambda x: x.text, tree.xpath('//abstract')) )[0]

# no webservice to just pull xml programatically, need the filesystem dirs and unzips?
# if needed, which part/parts should be threaded? the traversal? first get all paths, then spawn unzip(parse) for each

###
### traverse (walk through to each dir, run function when inside -- use os commands)
###

###
### unzip (parse run as callback when complete)
###

###
### parse (takes 'intellegence' data structure argument which provides necessary Elements and fields (can be changed and passed in at later dates as necessary) -> single dir piece of main state)
###

# assumes the right file in current context passed into tree...

def getFileInfo(indexFilename):
	return list(zip(map(lambda element: element.text, list(tree(indexFilename).xpath('//filename'))), map(lambda element: element.text, list(tree(indexFilename).xpath('//file-location')))))

def tree(filename):
	return etree.parse(filename, etree.XMLParser(encoding='utf-8', recover=True))

# get :: -> [Element]
def get(tree, elementPath):
	return tree.xpath(elementPath)

# getAll :: [{}], '' -> ?
def getAll(tree, element, info, alias):
	return list(map(lambda el: ( alias, el.tag, getattr(el, info) ), get(tree, element)))

# def get(Element, info): return Element[0][info] # works with standard dict, not sure what the one comming from tree is

# getAll :: Tree, [[ field, extraction, alias ]] -> [{}] 
def parse(tree, fields):
	return list(filter(lambda x: bool(x), list(map(lambda pair: getAll(tree, pair[0], pair[1], pair[2]), fields))))

def config(): 
	return [['//abstract', 'attrib', 'abstract'], ['//abstract', 'text', 'abstract'], ['//ep-patent-document', 'attrib', 'ep-patent-document'], ['//B001EP', 'text', 'nametocallit']]

def marker(id):
	return id + '-parsecomplete'

###
### supervisor (keep state via inital -- returns from parse, munge them together as necessary for final, how to handle failures)
###

# can parallelize/thread?

def process(config, xmlFile, archiveFile, callback):
  if not os.path.isfile(marker(xmlFile)):
    zipfile.ZipFile(archiveFile, "r").extract(xmlFile)
    
    # add validation/testing
    
    metadata = parse( tree(xmlFile), config() )
    
    # send off to queue (handle failures and restarts), if success, write file...
    callback(metadata)
    
    # gives us a durable state if the script breaks and reruns
    completefile = open(marker(xmlFile),"w") 
    completefile.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # state[file[0]] = 'info!'
    
    os.remove(xmlFile)  

def traverse(file, config, state, callback):
  initialDirectory = os.getcwd()
  workDirectory = file[1].replace('\\', '/')[1:]

  #print('init ' + initialDirectory)
  #print('work ' + workDirectory)

  archiveFile = file[0]
  xmlFile = file[0].replace('zip', 'xml')
 
  os.chdir(workDirectory)
  #print(os.getcwd())
  process(config, xmlFile, archiveFile, callback)
  os.chdir(initialDirectory) 

def run(path, config, state, callbackEach, callbackAll):
  fileInfo = getFileInfo(path)
  for file in fileInfo:
    traverse(file, config, state, callbackEach)
  callbackAll(state)

# import parser
# EPOparser.run('index.xml', EPOparser.config, {}, lambda x: x, lambda x: x)
# EPOparser.run('index.xml', EPOparser.config, {}, toQueue, print)

# capture doc id globally and state of document (kind code)... provide with each payload

