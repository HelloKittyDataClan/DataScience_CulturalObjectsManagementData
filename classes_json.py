import json

#UTF-8

#Specify the path to your JSON file
json_path = "1_CLASSES/process.json"

#Open and read the JSON file
with open(json_path, 'r') as file:
    json_data = file.read()

#Parse the JSON data into a Python object
data = json.loads(json_data)

#Utilize the data for UML purposes
#Print the data to inspect its structure

#print(data) --> controllare

#Classes and superclasses
#Application of methods with Python
class CulturalObject(object):
    def __init__(self, objects_id):
        self.id = set()
        for identifier in objects_id:
            self.id.add(identifier)

            self.objects_id = objects_id
            self.acquisition = None
            self.processing = None
            self.modelling = None
            self.optimising = None
            self.exporting = None
            
#Return a list of strings
    def getIds(self):
        result = []
        for identifier in self.id:
            result.append(identifier)
        result.sort()
        return result
    
    def addId(self, identifier):
        result = True
        if identifier not in self.id:
            self.id.add(identifier)
        else:
            result = False
        return result
    
    def removeId(self, identifier):
        result = True
        if identifier in self.id:
            self.id.remove(identifier)
        else:
            result = False
        return result

class Acquisition:
    def __init__(self, institute: str, person:str|list[str]|None=None, technique: str|list[str]|None= None, tools: str|list[str]|None= None, startDate:str= None, endDate:str= None):
        if person is not None and not isinstance(person, (str, list)):
            raise ValueError('Acquisition.person must be a string, a list of strings, or None')
        if not isinstance(technique, str):
            raise Exception('EntityWithMetadata.technique must be a string')
        if tools is not None and not isinstance(tools, (str, list)):
            raise ValueError('Acquisition.tools must be a string, a list of strings, or None')
        
        self.Institute = institute
        self.Person = person
        self.technique = technique
        self.tools = tools
        self.startDate = startDate
        self.endDate = endDate
    
    def getInstitute(self):
        return self.responsibleInstitute
    
    def getPerson(self):
        return self.responsiblePerson
    
    def getTechnique(self):
        return self.technique

    def getTool(self):
        return self.tools
    
    def getPeriod(self):
        return f"{self.startDate} to {self.endDate}"

class Processing:
    def __init__(self, institute, person, tools, startDate, endDate):
        self.Institute = institute
        self.Person = person
        self.tools = tools
        self.startDate = startDate
        self.endDate = endDate

class Modelling:
    def __init__(self, institute, person, tools, startDate, endDate):
        self.Institute = institute
        self.Person = person
        self.tools = tools
        self.startDate = startDate
        self.endDate = endDate

class Optimising:
    def __init__(self, institute, person, tools, startDate, endDate):
        self.Institute = institute
        self.Person = person
        self.tools = tools
        self.startDate = startDate
        self.endDate = endDate

class Exporting:
    def __init__(self, institute, person, tools, startDate, endDate):
        self.Institute = institute
        self.Person = person
        self.tools = tools
        self.startDate = startDate
        self.endDate = endDate

# Create instances for each object using the provided data
objects = []

for item in data:
    object = CulturalObject(item["object id"])
    object.acquisition = Acquisition(
        item["acquisition"]["institute"],
        item["acquisition"]["person"],
        item["acquisition"]["technique"],
        item["acquisition"]["tool"],
        item["acquisition"]["start date"],
        item["acquisition"]["end date"]
    )
    object.processing = Processing(
        item["processing"]["institute"],
        item["processing"]["person"],
        item["processing"]["tool"],
        item["processing"]["start date"],
        item["processing"]["end date"]
    )
    object.modelling = Modelling(
        item["modelling"]["institute"],
        item["modelling"]["person"],
        item["modelling"]["tool"],
        item["modelling"]["start date"],
        item["modelling"]["end date"]
    )
    object.optimising = Optimising(
        item["optimising"]["institute"],
        item["optimising"]["person"],
        item["optimising"]["tool"],
        item["optimising"]["start date"],
        item["optimising"]["end date"]
    )
    object.exporting = Exporting(
        item["exporting"]["institute"],
        item["exporting"]["person"],
        item["exporting"]["tool"],
        item["exporting"]["start date"],
        item["exporting"]["end date"]
    )
    objects.append(object)

# Now 'objects' contains instances of CulturalObject representing each object's data
# with its associated acquisition, processing, modelling, optimising, and exporting details.
