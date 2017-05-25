from lxml import etree
import os
import zipfile
import datetime
import functools

def fmap(fn, coll):
  return functools.reduce(lambda acc, val: acc + [fn(val)], coll, [])

def ffilter(predicateFn, coll):
  return functools.reduce(lambda acc, val: (acc + [val]) if predicateFn(val) else acc, coll, [])

def fcmap(fn):
  return lambda coll: functools.reduce(lambda acc, val: acc + [fn(val)], coll, [])

def fcfilter(predicateFn):
  return lambda coll: functools.reduce(lambda acc, val: (acc + [val]) if predicateFn(val) else acc, coll, [])

def fzip(coll1, coll2):
  return list(zip(coll1, coll2))

def fcompose(*functions):
  return functools.reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)  

###########
# helpers # 	
###########

def marker(id):
  return id + '-parsecomplete'

# fromElement :: attrib | text -> Element -> {} | String
def fromElement(info):
  return lambda el: getattr(el, info)

# tree :: String -> Tree
def tree(filename):
  return etree.parse(filename, etree.XMLParser(encoding='utf-8', recover=True))

# getFileInfo :: String -> [(filename, file-location)]
def getFileInfo(indexFilename):
  return fzip(
    fmap(fromElement('text'), getElement(tree(indexFilename), '//filename')),
    fmap(fromElement('text'), getElement(tree(indexFilename), '//file-location'))
  )

# getElement :: Tree, xpath -> [Element]
def getElement(tree, elementPath):
  return tree.xpath(elementPath)

# getMetadata :: Tree, xpath, attrib | text, String -> [] | [(alias, tag, {} | String)]
def getMetadata(tree, elementPath, info, alias):
  return fmap(lambda el: ( alias, el.tag, getattr(el, info) ), getElement(tree, elementPath))

# parse :: Tree, [[ xpath, attrib | text, String ]] -> [(alias, tag {} | String)]
def parse(tree, fields):
  return fcompose(
    fcmap(lambda x: x[0]),
    fcfilter(lambda x: bool(x)),
    fcmap(lambda field: getMetadata(tree, field[0], field[1], field[2]))
  )(fields)

##############
# processing #
##############

def process(fields, state, callback, xmlFile, archiveFile):
  if not os.path.isfile(marker(xmlFile)):
    zipfile.ZipFile(archiveFile, "r").extract(xmlFile)
    
    # add validation/testing
    
    metadata = parse( tree(xmlFile), fields() )
    
    # send off to queue (handle failures and restarts), if success, write file...
    callback(metadata)
    
    # gives us a durable state if the script breaks and reruns
    completefile = open(marker(xmlFile),"w") 
    completefile.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # gives us a final built object for logging/reporting, 
    # may not want if it is too large in memory!
    state[archiveFile] = metadata
    
    os.remove(xmlFile)  

def traverse(file, fields, state, callback):
  initialDirectory = os.getcwd()
  workDirectory = file[1].replace('\\', '/')[1:]

  #print('init ' + initialDirectory)
  #print('work ' + workDirectory)

  archiveFile = file[0]
  xmlFile = file[0].replace('zip', 'xml')
 
  os.chdir(workDirectory)
  #print(os.getcwd())
  process(fields, state, callback, xmlFile, archiveFile)
  os.chdir(initialDirectory) 

def fields(): 
  return [
    ['//abstract', 'attrib', 'abstract'], 
    ['//abstract', 'text', 'abstract'], 
    ['//ep-patent-document', 'attrib', 'ep-patent-document'], 
    ['//B001EP', 'text', 'nametocallit']
  ]

def run(path, fields, state, callbackEach, callbackAll):
  fileInfo = getFileInfo(path)
  for file in fileInfo:
    traverse(file, fields, state, callbackEach)
  callbackAll(state)

#################
# use and notes #
#################  

# import parser

# EPOParser.run('index.xml', EPOParser.fields, {}, lambda x: x, lambda x: x)
# EPOParser.run('index.xml', EPOParser.fields, {}, toQueue, print)

# capture doc id globally and state of document (kind code)... provide with each payload

# no webservice to just pull xml programatically, need the filesystem dirs and unzips?
# if needed, which part/parts should be threaded? the traversal? first get all paths, then spawn unzip(parse) for each


# parts that can be parallelized, threaded?
# processing/callbacks that can be sent IN to change the behavior at the callsite
# ...
